# -*- coding: utf-8 -*-
import logging

from h2.connection import (
    ConnectionInputs,
    ConnectionState,
    H2Connection,
    H2ConnectionStateMachine,
)


logger = logging.getLogger(__name__)


def patch_h2_transitions() -> None:
    """
    H2 transitions immediately to closed state upon receiving a GOAWAY frame.
    This is out of spec and results in errors when frames are received between
    the GOAWAY and us (the client) terminating the connection gracefully.
    We still terminate the connection following a GOAWAY.
    https://github.com/python-hyper/h2/issues/1181

    Instead of transitioning to CLOSED, remain in the current STATE and await
    a graceful termination.

    We also need to patch (noop) clear_outbound_data_buffer which would clear any
    outbound data.

    Fixes:
    RPC terminated with:
    code = StatusCode.UNAVAILABLE
    message = "Invalid input ConnectionInputs.RECV_PING in state ConnectionState.CLOSED"
    status = "code: 14
    """
    logger.info("H2 transitions patched for RECV_GOAWAY frame fix")

    patched_transitions = {
        # State: idle
        (ConnectionState.IDLE, ConnectionInputs.RECV_GOAWAY): (
            None,
            ConnectionState.IDLE,
        ),
        # State: open, client side.
        (ConnectionState.CLIENT_OPEN, ConnectionInputs.RECV_GOAWAY): (
            None,
            ConnectionState.CLIENT_OPEN,
        ),
        # State: open, server side.
        (ConnectionState.SERVER_OPEN, ConnectionInputs.RECV_GOAWAY): (
            None,
            ConnectionState.SERVER_OPEN,
        ),
        (ConnectionState.IDLE, ConnectionInputs.SEND_GOAWAY): (
            None,
            ConnectionState.IDLE,
        ),
        (ConnectionState.CLIENT_OPEN, ConnectionInputs.SEND_GOAWAY): (
            None,
            ConnectionState.CLIENT_OPEN,
        ),
        (ConnectionState.SERVER_OPEN, ConnectionInputs.SEND_GOAWAY): (
            None,
            ConnectionState.SERVER_OPEN,
        ),
    }

    H2ConnectionStateMachine._transitions.update(patched_transitions)

    # no op this method which is called by h2 after recieving a GO_AWAY frame
    def clear_outbound_data_buffer(*args, **kwargs):
        pass

    H2Connection.clear_outbound_data_buffer = clear_outbound_data_buffer
