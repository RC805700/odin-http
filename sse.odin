package http

import "core:bytes"
import "core:container/queue"
import "core:log"
import "core:nbio"
import "core:net"
import "core:strings"


// TODO: shutdown doesn't work.

Sse :: struct {
	user_data: rawptr,
	on_err:    Maybe(Sse_On_Error),
	r:         ^Response,

	// State should be considered read-only by users.
	state:     Sse_State,
	_events:   queue.Queue(Sse_Event),
	_buf:      strings.Builder,
	_sent:     int,
	
	// Pointer to the owning thread's connection map (for shutdown filtering)
	owner_conns: ^map[net.TCP_Socket]^Connection,
}

Sse_Event :: struct {
	event:   Maybe(string),
	data:    Maybe(string),
	id:      Maybe(string),
	retry:   Maybe(int),
	comment: Maybe(string),
}

Sse_State :: enum {
	Pre_Start,

	// The initial HTTP response is being sent over the connection (status code&headers) before
	// we can start sending events.
	Starting,

	// No events are being sent over the connection but it is ready to.
	Idle,

	// An event is being sent over the connection.
	Sending,

	// Set to when sse_end is called when there are still events in the queue.
	// The events in the queue will be processed and then closed.
	Ending,

	// Either done ending or forced ending.
	// Every callback will return immediately, nothing else is processed.
	Close,
}

Sse_On_Error :: #type proc(sse: ^Sse, err: nbio.Send_Error)

sse_init :: proc(
	sse: ^Sse,
	r: ^Response,
	user_data: rawptr = nil,
	on_error: Maybe(Sse_On_Error) = nil,
	allocator := context.temp_allocator,
) {
	sse.r = r
	sse.user_data = user_data
	sse.on_err = on_error
	sse.owner_conns = &td.conns

	queue.init(&sse._events, allocator = allocator)
	strings.builder_init(&sse._buf, allocator)

	if r.status == .Not_Found {r.status = .OK}
	if !headers_has_unsafe(r.headers, "content-type") {
		headers_set_unsafe(&r.headers, "content-type", "text/event-stream")
	}
}

sse_start :: proc(sse: ^Sse, loc := #caller_location) {
	assert_has_td(loc)

	sse.state = .Starting
	_response_write_heading(sse.r, -1)

	on_start_send :: proc(op: ^nbio.Operation, sse: ^Sse) {
		if op.send.err != nil {
			_sse_err(sse, op.send.err)
			return
		}

		_sse_process(sse)
	}

	buf := bytes.buffer_to_bytes(&sse.r._buf)
	nbio.send_poly(sse.r._conn.socket, {buf}, sse, on_start_send)
}

sse_event :: proc(sse: ^Sse, ev: Sse_Event, loc := #caller_location) {
	assert_has_td(loc)

	switch sse.state {
	case .Starting, .Sending, .Ending, .Idle:
		queue.push_back(&sse._events, ev)

	case .Pre_Start:
		panic("sse_start must be called first", loc)

	case .Close:
	}

	if sse.state == .Idle {
		_sse_process(sse)
	}
}

sse_end_force :: proc(sse: ^Sse) {
	sse.state = .Close

	_sse_call_on_err(sse, {})
	sse_destroy(sse)
	connection_close(sse.r._conn)
}

sse_end :: proc(sse: ^Sse) {
	if sse.state >= .Ending {return}

	if sse.state == .Sending {
		sse.state = .Ending
		return
	}

	sse.state = .Close

	_sse_call_on_err(sse, {})
	sse_destroy(sse)
	connection_close(sse.r._conn)
}

sse_destroy :: proc(sse: ^Sse) {
	strings.builder_destroy(&sse._buf)
	queue.destroy(&sse._events)
}

_sse_err :: proc(sse: ^Sse, err: nbio.Send_Error) {
	if sse.state >= .Ending {return}

	sse.state = .Close

	_sse_call_on_err(sse, err)
	sse_destroy(sse)
	connection_close(sse.r._conn)
}

_sse_call_on_err :: proc(sse: ^Sse, err: nbio.Send_Error) {
	if cb, ok := sse.on_err.?; ok {
		cb(sse, err)
	} else if err != nil {
		log.infof("Server Sent Event error: %v", err)
	}
}

_sse_process :: proc(sse: ^Sse) {
	if sse.state == .Close {return}

	if queue.len(sse._events) == 0 {
		#partial switch sse.state {
		case .Ending:
			sse_end_force(sse)
		case:
			sse.state = .Idle
		}
		return
	}

	#partial switch sse.state {
	case .Ending:
	case:
		sse.state = .Sending
	}

	_sse_event_prepare(sse)
	nbio.send_poly(sse.r._conn.socket, {sse._buf.buf[:]}, sse, _sse_on_send)
}

_sse_on_send :: proc(op: ^nbio.Operation, sse: ^Sse) {
	if op.send.err != nil {
		_sse_err(sse, op.send.err)
		return
	}

	if sse.state == .Close {return}

	queue.pop_front(&sse._events)
	_sse_process(sse)
}

_sse_event_prepare :: proc(sse: ^Sse) {
	ev := queue.front(&sse._events)
	b := &sse._buf

	strings.builder_reset(b)
	sse._sent = 0

	if ev.event != nil {
		strings.write_string(b, "event: ")
		strings.write_string(b, ev.event.?)
		strings.write_string(b, "\r\n")
	}

	if ev.comment != nil {
		strings.write_string(b, "; ")
		strings.write_string(b, ev.comment.?)
		strings.write_string(b, "\r\n")
	}

	if ev.id != nil {
		strings.write_string(b, "id: ")
		strings.write_string(b, ev.id.?)
		strings.write_string(b, "\r\n")
	}

	if ev.retry != nil {
		strings.write_string(b, "retry: ")
		strings.write_int(b, ev.retry.?)
		strings.write_string(b, "\r\n")
	}

	if ev.data != nil {
		strings.write_string(b, "data: ")
		strings.write_string(b, ev.data.?)
		strings.write_string(b, "\r\n")
	}

	strings.write_string(b, "\r\n")
}
