import argparse
import asyncio
import json
import logging
import struct
import sys
import zmq
import zmq.asyncio
import zmapi.utils
from numbers import Number
from itertools import count
from asyncio import ensure_future as create_task
from bidict import bidict
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
from pprint import pprint, pformat
from sortedcontainers import SortedDict
from time import sleep, time, gmtime
from zmapi.exceptions import *
from zmapi.logging import setup_root_logger, disable_logger
from zmapi.zmq import ReturningDealer
from zmapi.zmq.utils import split_message, ident_to_str
from zmapi.controller import Controller, MiddlewareCTL
from zmapi.utils import partition, send_recv_command_raw, delayed
from zmapi import fix, Publisher, Subscriber
from uuid import uuid4


################################## CONSTANTS ##################################


# CAPABILITIES = [
#     fix.ZMCap.ListEndpoints,
# ]


################################### GLOBALS ###################################

L = logging.root

class GlobalState:
    pass
g = GlobalState()

g.loop = asyncio.get_event_loop()
g.ctx = zmq.asyncio.Context()

# def create_empty_ep_data():
#     return {
#         "sock_dealer": None,
#         "sock_sub": None,
#         "dealer": None,
#         "subscriber": None,
#     }

g.eps = defaultdict(dict)
# g.sub_to_ep = {}

g.startup_event = asyncio.Event()


###############################################################################


class MyController(Controller):


    def __init__(self, sock_dn, publisher):
        super().__init__(sock_dn)
        self._pub = publisher


    async def TestRequest(self, ident, msg_raw, msg):
        ep = msg["Header"].get("ZMEndpoint")
        if ep:
            ep_data = g.eps[ep]
            res = await ep_data["dealer"].send_recv_msg(msg_raw, ident=ident)
            return res[-1]
        body = msg["Body"]
        res = {}
        res["Header"] = {"MsgType": fix.MsgType.Heartbeat}
        tr_id = body.get("TestReqID")
        res["Body"] = {}
        if tr_id:
            res["Body"]["TestReqID"] = tr_id
        if body.get("ZMSendToPub"):
            pub_msg = {}
            pub_msg["Header"] = {
                "ZMSendingTime": int(datetime.utcnow().timestamp() * 1e9),
                "MsgSeqNum": g.seq_no,
            }
            g.seq_no += 1
            pub_msg["Body"] = {}
            if tr_id:
                pub_msg["Body"]["TestReqID"] = tr_id
            pub_msg_bytes = (" " + json.dumps(pub_msg)).encode()
            await g.sock_pub.send_multipart(
                    [fix.MsgType.Heartbeat.encode(), pub_msg_bytes])
        return res


    ResendRequest = MiddlewareCTL.ResendRequest


    async def ZMGetSessionID(self, ident, msg_raw, msg):
        res = {}
        res["Header"] = {"MsgType": fix.MsgType.ZMGetSessionIDResponse}
        res["Body"] = {"ZMSessionID": self.session_id}
        return res


    # async def ZMListCapabilities(self, ident, msg_raw, msg):
    #     ep = msg["Header"].get("ZMEndpoint")
    #     if ep:
    #         socks = g.eps[ep]
    #         res = await socks["dealer"].send_recv_msg(msg_raw, ident=ident)
    #         return res[-1]
    #     res = {}
    #     res["Header"] = header = {}
    #     header["MsgType"] = fix.MsgType.ZMListCapabilitiesResponse
    #     res["Body"] = body = {}
    #     body["ZMCaps"] = CAPABILITIES
    #     return res


    async def ZMListEndpoints(self, ident, msg_raw, msg):
        res = {}
        res["Header"] = {"MsgType": fix.MsgType.ZMListEndpointsResponse}
        res["Body"] = body = {}
        body["ZMEndpoints"] = sorted(g.eps)
        return res


    # TODO: add commands for dynamically adding/removing upstream connections


    async def _handle_msg_2(self, ident, msg_raw, msg, msg_type):
        f = self._commands.get(msg_type)
        if f:
            return await f(self, ident, msg_raw, msg)
        if not f:
            ep = msg["Header"].get("ZMEndpoint")
            if not ep:
                raise RejectException("missing ZMEndpoint field")
            if ep not in g.eps:
                raise RejectException("unknown ZMEndpoint")
            ep_data = g.eps[ep]
            res = await ep_data["dealer"].send_recv_msg(msg_raw, ident=ident)
            return res[-1]


    async def _handle_msg_1(self, ident, msg_id, msg_raw):
        msg = json.loads(msg_raw.decode())
        msg_type = msg["Header"]["MsgType"]
        debug_str = "ident={}, MsgType={}, msg_id={}"
        debug_str = debug_str.format(
                ident_to_str(ident), msg_type, msg_id)
        ep = msg["Header"].get("ZMEndpoint")
        if ep:
            debug_str += f", ep={ep}"
        L.debug(self._tag + "> " + debug_str)
        try:
            res = await self._handle_msg_2(
                    ident, msg_raw, msg, msg_type)
        except RejectException as e:
            L.exception(self._tag + "ZMReject processing {}: {}"
                        .format(msg_id, str(e)))
            text, reason, field_name = e.args
            await self._send_xreject(ident,
                                     msg_id,
                                     reason,
                                     text,
                                     field_name)
        except Exception as e:
            L.exception(self._tag + "Generic ZMReject processing {}: {}"
                        .format(msg_id, str(e)))
            await self._send_xreject(ident,
                                     msg_id,
                                     fix.ZMRejectReason.Other,
                                     "{}: {}".format(type(e).__name__, e))
        else:
            if res is not None:
                await self._send_reply(ident, msg_id, res)
        L.debug(self._tag + "< " + debug_str)


###############################################################################


class MySubscriber(Subscriber):


    def __init__(self, sock, endpoint, dealer=None, name=None):
        super().__init__(sock, dealer=dealer, name=name)
        self._endpoint = endpoint
        self._topic = endpoint.encode() + b"\0"


    async def _handle_msg_2(self, topic, msg):
        if topic is None:
            topic = self._topic
        else:
            topic = topic.encode() + b"\0" + self._topic
        msg["Header"]["ZMEndpoint"] = self._endpoint
        if "MsgSeqNum" in msg["Header"]:
            no_seq_num = False
        else:
            no_seq_num = True
        L.debug("publishing topic: {}".format(topic))
        await g.pub.publish_msg(msg, topic, no_seq_num=no_seq_num)


###############################################################################



# def add_endpoint(ep, dealer, sock_sub):
#     if ep in g.eps:
#         for i in count(2):
#             ep_mod = "{}{:03d}".format(ep, i)
#             if ep_mod not in g.eps:
#                 ep = ep_mod
#                 break
#     g.eps[ep]["dealer"] = dealer
#     g.eps[ep]["sock_sub"] = sock_sub
#     L.info("endpoint added: {}".format(ep))

def get_endpoint_name(ep):
    if ep in g.eps:
        for i in count(2):
            ep_mod = "{}{:03d}".format(ep, i)
            if ep_mod not in g.eps:
                return ep_mod
    return ep

# def get_eps_from_status(data):
#     end = data[-1]
#     if type(end) is dict:
#         return [end["endpoint_name"]]
#     res = []
#     for chain in end:
#         res += get_eps_from_status(chain)
#     return res


CHAIN_MOD_LOCK = asyncio.Lock()
async def init_new_upstream_chain(ctl_addr, pub_addr, timeout=None):
    async with CHAIN_MOD_LOCK:
        L.info("initializing upstream connection (CTL: {} / PUB: {}) ..."
               .format(ctl_addr, pub_addr))
        sock_deal = g.ctx.socket(zmq.DEALER)
        sock_deal.connect(ctl_addr)
        dealer = ReturningDealer(g.ctx, sock_deal)
        sock_sub = g.ctx.socket(zmq.SUB)
        sock_sub.connect(pub_addr)
        sock_sub.subscribe(b"")
        res = await send_recv_command_raw(
                sock_deal, fix.MsgType.ZMListEndpoints, timeout=timeout,
                check_error=True)
        res = res["Body"]["ZMNoEndpoints"]
        if len(res) != 1:
            raise RuntimeError("expected exactly one endpoint, got {}"
                               .format(len(res)))
        endpoint = endpoint_raw = res[0]
        endpoint = get_endpoint_name(endpoint)
        L.info(f"endpoint added: {endpoint_raw} -> {endpoint}")
        state = g.eps[endpoint]
        state["dealer"] = dealer
        state["subscriber"] = MySubscriber(
                sock_sub, endpoint, dealer=dealer, name="sub-"+endpoint)
        create_task(dealer.run())
        create_task(state["subscriber"].run())
        # print("sending wakeup ...")
        # await g.pub_fwd_sentinel_notifier.send(b"WAKEUP")


# async def fwd_pub_msg(sock):
#     ep = g.sub_to_ep[sock]
#     msg_parts = await sock.recv_multipart()
#     msg_type = msg_parts[0].split(b"\x00")[0]
#     topic_dn = msg_type + b"\x00" + ep.encode()
#     await g.sock_pub.send_multipart([topic_dn, msg_parts[1]])


# async def pub_forwarder():
# 
#     L.debug("pub_forwarder running ...")
# 
#     poller = zmq.asyncio.Poller()
#     poller.register(g.pub_fwd_sentinel)
# 
#     def refresh_pub_poller():
#         for socks in g.eps.values():
#             poller.register(socks["sub"], zmq.POLLIN)
#         L.debug("refreshed pub_fwd poller")
# 
#     # make sure poller is ready before startup_event is set
#     while True:
#         try:
#             await g.pub_fwd_sentinel.recv(zmq.NOBLOCK)
#         except:
#             break
#     refresh_pub_poller()
#     g.startup_event.set()
# 
#     while True:
# 
#         poll_res = await poller.poll()
#         socks, _ = zip(*poll_res)
# 
#         for sock in socks:
#             if sock == g.pub_fwd_sentinel:
#                 await sock.recv()
#                 refresh_pub_poller()
#             else:
#                 try:
#                     await fwd_pub_msg(sock)
#                 except Exception:
#                     L.exception("error forwarding pub msg:")


###############################################################################


def parse_args():
    desc = "hub middleware module"
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("ctl_addr_down",
                        help="ctl socket binding address")
    parser.add_argument("pub_addr_down",
                        help="pub socket binding address")
    parser.add_argument("upstream_addresses", nargs="+",
                        help="pairs of CTL and PUB addresses for upstream"
                             " branches")
    parser.add_argument("--log-level", default="INFO", help="logging level")
    args = parser.parse_args()
    try:
        args.log_level = int(args.log_level)
    except ValueError:
        pass
    return args


def setup_logging(args):
    setup_root_logger(args.log_level)
    disable_logger("parso.python.diff")
    disable_logger("parso.cache")


async def init_zmq_sockets(args):

    g.pub_fwd_sentinel_notifier = g.ctx.socket(zmq.PUSH)
    g.pub_fwd_sentinel = g.ctx.socket(zmq.PULL)
    g.pub_fwd_sentinel.bind("inproc://pub_fwd_sentinel")
    g.pub_fwd_sentinel_notifier.connect("inproc://pub_fwd_sentinel")

    g.sock_ctl = g.ctx.socket(zmq.ROUTER)
    g.sock_ctl.bind(args.ctl_addr_down)
    g.sock_pub = g.ctx.socket(zmq.PUB)
    g.sock_pub.bind(args.pub_addr_down)

    pairs = partition(args.upstream_addresses, 2, complete_only=True)
    for ctl_addr, pub_addr in pairs:
        await init_new_upstream_chain(ctl_addr, pub_addr)


def main():
    args = parse_args()
    g.args = args
    setup_logging(args)
    g.loop.run_until_complete(init_zmq_sockets(args))
    g.pub = Publisher(g.sock_pub)
    g.ctl = MyController(g.sock_ctl, g.pub)
    tasks = [
        g.ctl.run(),
        #delayed(g.ctl.run, g.startup_event),
        #pub_forwarder(),
    ]
    tasks = [create_task(coro_obj) for coro_obj in tasks]
    try:
        g.loop.run_until_complete(asyncio.gather(*tasks))
    except KeyboardInterrupt:
        pass
    g.ctx.destroy()


if __name__ == "__main__":
    main()
