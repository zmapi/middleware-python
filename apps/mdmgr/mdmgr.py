import zmq
import zmq.auth
import zmq.asyncio
import asyncio
from asyncio import ensure_future as create_task
import sys
import logging
import json
import argparse
import zmapi.utils
from zmapi.zmq.utils import split_message, ident_to_str
from zmapi.logging import setup_basic_logging, disable_logger
from zmapi.utils import (check_missing, delayed, count_num_decimals,
                         get_timestamp)
from zmapi.zmq import ReturningDealer
from zmapi.controller import MiddlewareCTL
from zmapi import fix, Publisher, Subscriber
from zmapi.exceptions import *
import uuid
from sortedcontainers import SortedDict
from collections import defaultdict
from time import gmtime
from pprint import pprint, pformat
from datetime import datetime
from copy import deepcopy
from base64 import b64encode, b64decode

# TASKS
# - cache all data for snapshots
# - if sync snapshots are supported natively on particular datatype,
#   try requesting snapshots at startup to see if it can be done
# - handle unsync snapshots natively if possible, otherwise simply return cache
# - keep track of maximum subscription for each target and remove dead clients
#   and their subscriptions
# - named subscriptions:
#   - keep track of named subscriptions and unsubscribe dead clients'
#     subscriptions
#   - translation between client-specific subnames and global subnames
# - use RefreshIndicator when publishing redundant snapshots (False for
#   redundant data)


################################## CONSTANTS ##################################


INTERNAL_PUB_ADDR = "inproc://internal-pub"


################################### GLOBALS ###################################


L = logging.root

class GlobalState:
    pass
g = GlobalState()
g.loop = asyncio.get_event_loop()
g.ctx = zmq.asyncio.Context.instance()

def _create_empty_ins_data_dict():
    def _create_mbp_mbo():
        return {
            "mbp": {
                # each price level has two entries: "implied" and "normal"
                "bids": SortedDict(lambda x: -x),
                "asks": SortedDict(),
            },
            "mbo": {},
        }
    def _create_market_dict():
        return {
            "lot_type": defaultdict(_create_mbp_mbo),
            "generic_md": {},
            # "subscribed": False,
        }
    return {
        # key: (ZMMarketID, ZMTradingSessionID)
        "markets": defaultdict(_create_market_dict),
        "status": None,  # SecurityStatus goes here
        "r2i": None,
        "i2r": None,
        "def": None,
        "def_tt": None,
    }
# key: ZMInstrumentID
g.ins_data = defaultdict(_create_empty_ins_data_dict)

# for MarketDefinitionRequest subscription
# key: ZMMarketID
g.market_defs = {}

# for TradingSessionStatusRequest subscription
# key: ZMTradingSessionID
g.ts_status = {}

g.next_req_id_prefix = 0


###############################################################################


class Subscription(dict):


    def __init__(self, d=None):
        super().__init__()
        if d is not None:
            self.merge(d, inplace=True)

    
    def merge(self, x, inplace=False):
        res = self._merge2(x)
        if inplace:
            self.clear()
            self.update(res)
        else:
            return self.__class__(res)


    def _merge2(self, x):
        raise NotImplementedError("must be implemented")


    def contains(self, x):
        raise NotImplementedError("must be implemented")


class MDSubscription(Subscription):


    def _merge2(self, x):
        res = {}
        if self.get("MarketDepth", 1) == 0 or x["MarketDepth"] == 0:
            res["MarketDepth"] = 0
        else:
            res["MarketDepth"] = max(self.get("MarketDepth", 1),
                                      x["MarketDepth"])
        if fix.ZMCap.MDMBPPlusMBO in g.caps:
            res["ZMMBOBook"] = self.get("ZMMBOBook", False) | x["ZMMBOBook"]
            res["ZMMBPBook"] = self.get("ZMMBPBook", False) | x["ZMMBPBook"]
        else:
            res["AggregatedBook"] = min(self.get("AggregatedBook", True),
                                        x.get("AggregatedBook", True))
        if "*" in self.get("NoMDEntryTypes", "") or "*" in x["NoMDEntryTypes"]:
            res["NoMDEntryTypes"] = "*"
        else:
            ets = set(self.get("NoMDEntryTypes", "") + x["NoMDEntryTypes"])
            res["NoMDEntryTypes"] = "".join(sorted(ets))
        # if "NoTradingSessions" in x and "NoTradingSessions" in self:
        #     group = []
        #     group += self["NoTradingSessions"]
        #     for ts_b in x["NoTradingSessions"]:
        #         for ts_a in self["NoTradingSessions"]:
        #             if ts_b == ts_a:
        #                 break
        #         else:
        #             group.append(ts_b)
        #     res["NoTradingSessions"] = group
        return res


    def contains(self, x):
        if self["MarketDepth"] > 0 and x["MarketDepth"] > self["MarketDepth"]:
            return False
        if fix.ZMCap.MDMBPPlusMBO in g.caps:
            if not self["ZMMBOBook"] and x["ZMMBOBook"]:
                return False
            if not self["ZMMBPBook"] and x["ZMMBPBook"]:
                return False
        else:
            if self["AggregatedBook"] and not x["AggregatedBook"]:
                return False
        if "*" not in self["NoMDEntryTypes"]:
            if set(x["NoMDEntryTypes"]).difference(x["NoMDEntryTypes"]):
                return False
        if "NoTradingSessions" in self:
            if "NoTradingSessions" not in x:
                return False
            for ts_b in x["NoTradingSessions"]:
                for ts_a in self["NoTradingSessions"]:
                    if ts_b == ts_a:
                        break
                else:
                    return False
        return True


class ParameterlessSubscription(Subscription):


    def _merge2(self, x):
        return {}


    def contains(self, x):
        return True


class SubscriptionManager:


    def __init__(self):
        def create_client_data():
            return {
                "ins_md_subs": defaultdict(dict),
                "market_def_subs": defaultdict(dict),
                "ts_status_subs": defaultdict(dict),
                "ins_def_subs": defaultdict(dict),
                "ins_status_subs": defaultdict(dict),
                "health": g.timeout_heartbeats,
                "sec_list_subs": {},
            }
        self._client_data = defaultdict(create_client_data)
        def create_empty_sub_dict():
            return SortedDict(lambda x: x[0])
        self._ins_md_subs = defaultdict(create_empty_sub_dict)
        self.ins_md_subs_merged = {}
        self._market_def_subs = defaultdict(create_empty_sub_dict)
        self.market_def_subs_merged = {}
        self._ts_status_subs = defaultdict(create_empty_sub_dict)
        self.ts_status_subs_merged = {}
        self._ins_def_subs = defaultdict(create_empty_sub_dict)
        self.ins_def_subs_merged = {}
        self._ins_status_subs = defaultdict(create_empty_sub_dict)
        self.ins_status_subs_merged = {}

    
    # @staticmethod
    # def _merge_md_subs(a, b):
    #     res = {}
    #     if a["MarketDepth"] == 0 or b["MarketDepth"] == 0:
    #         res["MarketDepth"] = 0
    #     else:
    #         res["MarketDepth"] = max(a["MarketDepth"], b["MarketDepth"])
    #     if fix.ZMCap.MDMBPPlusMBO in g.caps:
    #         res["ZMMBOBook"] = a["ZMMBOBook"] | b["ZMMBOBook"]
    #         res["ZMMBPBook"] = a["ZMMBPBook"] | b["ZMMBPBook"]
    #     else:
    #         res["AggregatedBook"] = min(a.get("AggregatedBook", True),
    #                                     b.get("AggregatedBook", True))
    #     if "*" in a["NoMDEntryTypes"] or "*" in b["NoMDEntryTypes"]:
    #         res["NoMDEntryTypes"] = "*"
    #     else:
    #         ets = set(a["NoMDEntryTypes"] + b["NoMDEntryTypes"])
    #         res["NoMDEntryTypes"] = "".join(sorted(ets))
    #     return res


    # @staticmethod
    # def _md_sub_contains(a, b):
    #     if a["MarketDepth"] > 0 and b["MarketDepth"] > a["MarketDepth"]:
    #         return False
    #     if fix.ZMCap.MDMBPPlusMBO in g.caps:
    #         if not a["ZMMBOBook"] and b["ZMMBOBook"]:
    #             return False
    #         if not a["ZMMBPBook"] and b["ZMMBPBook"]:
    #             return False
    #     else:
    #         if a["AggregatedBook"] and not b["AggregatedBook"]:
    #             return False
    #         # if not a["MarketDepth"] and b["MarketDepth"]:
    #         #     return False
    #     if "*" not in a["NoMDEntryTypes"]:
    #         if b["NoMDEntryTypes"].difference(a["NoMDEntryTypes"]):
    #             return False
    #     return True


    # def _merge_all_md_subs(self, subs, sub_cmp=None):
    #     merged = None
    #     ts_contained = None
    #     for (ts, _), d in subs.items():
    #         if merged is None:
    #             merged = d
    #         else:
    #             merged = self._merge_md_subs(merged, d)
    #         if sub_cmp \
    #                 and ts_contained is None \
    #                 and self._md_sub_contains(merged, sub_cmp):
    #             ts_contained = ts
    #     return merged, ts_contained


    @staticmethod
    def _merge_subs(subs, sub_cmp=None):
        merged = None
        ts_contained = None
        for (ts, _), d in subs.items():
            if merged is None:
                merged = d
            else:
                merged = merged.merge(d)
            if sub_cmp \
                    and ts_contained is None \
                    and merged.contains(sub_cmp):
                ts_contained = ts
        return merged, ts_contained


    @staticmethod
    def _update_upstream_sub_base(new_sub):
        if new_sub is None:
            srt = fix.SubscriptionRequestType.Unsubscribe
        else:
            srt = fix.SubscriptionRequestType.SnapshotAndUpdates
        body = {} if new_sub is None else deepcopy(new_sub)
        body["SubscriptionRequestType"] = srt
        return body



    # async def _update_upstream_md_sub(
    #         self, ins_id, market_id, market_seg_id, new_sub):
    #     market_key = (market_id, market_seg_id)
    #     if not new_sub:
    #         srt = fix.SubscriptionRequestType.Unsubscribe
    #         L.debug(f"unsubscribing upstream: {ins_id} {market_key}")
    #     else:
    #         srt = fix.SubscriptionRequestType.SnapshotAndUpdates
    #         L.debug(f"subscribing upstream: {ins_id} {market_key}")
    #     body = {} if new_sub is None else deepcopy(new_sub)
    #     body["ZMInstrumentID"] = ins_id
    #     if market_id:
    #         body["MarketID"] = market_id
    #     if market_seg_id:
    #         body["MarketSegmentID"] = market_seg_id
    #     body["SubscriptionRequestType"] = srt
    #     res = await g.ctl.send_recv_command(
    #             fix.MsgType.MarketDataRequest,
    #             body=body,
    #             check_error=True)
    #     return res


    # async def _update_upstream_simple_sub(self, ins_id, subscribe, msg_type):
    #     if subscribe:
    #         srt = fix.SubscriptionRequestType.SnapshotAndUpdates
    #     else:
    #         srt = fix.SubscriptionRequestType.Unsubscribe
    #     body = {}
    #     body["SubscriptionRequestType"] = srt
    #     body["ZMInstrumentID"] = ins_id
    #     res = await g.ctl.send_recv_command(
    #             msg_type, body=body, check_error=True)
    #     return res


    # async def _update_upstream_ins_def_sub(self, ins_id, subscribe):
    #     return await self._update_upstream_simple_sub(
    #             ins_id,
    #             subscribe,
    #             fix.MsgType.SecurityDefinition)


    # async def _update_upstream_ins_status_sub(self, ins_id, subscribe):
    #     return await self._update_upstream_simple_sub(
    #             ins_id,
    #             subscribe,
    #             fix.MsgType.SecurityStatus)

    async def _update_sub(self,
                          sub,
                          srt,
                          subs_by_key,
                          sub_key,
                          ident_key,
                          cd_subs,
                          res_msg_type,
                          us_update_afn,
                          subs_merged):

        assert sub is not None
        ts_now = get_timestamp()
        ts_contained = None
        
        prev_sub_entry = None
        for k in subs_by_key.keys():
            if k[1] == ident_key:
                prev_sub_entry = (k, subs_by_key[k])
                break

        if srt == fix.SubscriptionRequestType.Unsubscribe:
            if prev_sub_entry is None:
                raise ValueError("no subscription found")
            old_merged_sub, _ = self._merge_subs(subs_by_key)
            del subs_by_key[prev_sub_entry[0]]
            new_merged_sub, _ = self._merge_subs(subs_by_key)
            if new_merged_sub != old_merged_sub:
                subs_merged[sub_key] = new_merged_sub
                try:
                    res = await us_update_afn(new_merged_sub)
                except RejectException:
                    subs_by_key[prev_sub_entry[0]] = prev_sub_entry[1]
                    subs_merged[sub_key] = old_merged_sub
                    raise
            else:
                res = {}
                res["Header"] = {"MsgType": res_msg_type}
                res["Body"] = {}
                res["Body"]["Text"] = "no change in upstream subscription"
            del cd_subs[sub_key]

        else:
            old_merged_sub, ts_contained = self._merge_subs(subs_by_key, sub)
            if ts_contained is None:
                ts_contained = ts_now
            subs_by_key[(ts_now, ident_key)] = sub
            new_merged_sub, _ = self._merge_subs(subs_by_key)
            if new_merged_sub != old_merged_sub:
                subs_merged[sub_key] = new_merged_sub
                try:
                    res = await us_update_afn(new_merged_sub)
                except RejectException:
                    del subs_by_key[(ts_now, ident_key)]
                    if prev_sub_entry:
                        subs_by_key[prev_sub_entry[0]] = prev_sub_entry[1]
                    subs_merged[sub_key] = old_merged_sub
                    raise
            else:
                res = {}
                res["Header"] = {"MsgType": res_msg_type}
                res["Body"] = {}
                res["Body"]["Text"] = "no change in upstream subscription"
            res["Body"]["_TimeSubscribed"] = ts_contained
            cd_subs[sub_key] = sub

        return res, old_merged_sub, new_merged_sub


    async def update_ins_md_sub(self, ident, msg):
        body = msg["Body"]
        srt = body["SubscriptionRequestType"]
        if srt == fix.SubscriptionRequestType.SnapshotAndUpdates:
            sub = MDSubscription(body)
        else:
            sub = body
        ins_id = body["ZMInstrumentID"]
        market_id = body.get("ZMMarketID", None)
        ts_id = body.get("ZMTradingSessionID", None)
        sub_key = (ins_id,
                   market_id,
                   ts_id)
        subs_by_key = self._ins_md_subs[sub_key]
        ident_key = tuple(ident)
        cd_subs = self._client_data[ident_key]["ins_md_subs"]
        async def us_update_afn(new_sub):
            body = self._update_upstream_sub_base(new_sub)
            body["ZMInstrumentID"] = ins_id
            if market_id:
                body["ZMMarketID"] = market_id
            if ts_id:
                body["ZMTradingSessionID"] = ts_id
            res = await g.ctl.send_recv_command(
                    fix.MsgType.MarketDataRequest,
                    body=body,
                    check_error=True)
            return res
        res, old, new = (await self._update_sub(
                sub=sub,
                srt=srt,
                subs_by_key=subs_by_key,
                sub_key=sub_key,
                ident_key=ident_key,
                cd_subs=cd_subs,
                res_msg_type=fix.MsgType.ZMMarketDataRequestResponse,
                us_update_afn=us_update_afn,
                subs_merged=self.ins_md_subs_merged))
        if new is None:
            g.ins_data[ins_id]["markets"].pop((market_id, ts_id), None)
        else:
            md = g.ins_data[ins_id]["markets"][(market_id, ts_id)]
            L.debug(f'before:\n{md}')
            ets = set(new["NoMDEntryTypes"])
            if "*" not in ets:
                for et in md["generic_md"]:
                    if et not in ets:
                        del md["generic_md"][et]
            if fix.MDEntryType.Bid not in ets:
                for books in md["lot_type"].values():
                    books["mbo"] = {k: v for k, v in books["mbo"].items()
                                    if v["MDEntryType"] != fix.MDEntryType.Bid}
                    book = books["mbp"]["bids"]
                    for k, lvls in book.items():
                        lvls.pop("normal", None)
            if fix.MDEntryType.Offer not in ets:
                for books in md["lot_type"].values():
                    books["mbo"] = {k: v for k, v in books["mbo"].items()
                                    if v["MDEntryType"] != fix.MDEntryType.Offer}
                    book = books["mbp"]["asks"]
                    for k, lvls in book.items():
                        lvls.pop("normal", None)
            if fix.MDEntryType.SimulatedSellPrice not in ets:
                for books in md["lot_type"].values():
                    book = books["mbp"]["bids"]
                    for k, lvls in book.items():
                        lvls.pop("implied", None)
            if fix.MDEntryType.SimulatedBuyPrice not in ets:
                for books in md["lot_type"].values():
                    book = books["mbp"]["asks"]
                    for k, lvls in book.items():
                        lvls.pop("implied", None)
            if fix.ZMCap.MDMBPPlusMBO in g.caps:
                if not new["ZMMBOBook"]:
                    for books in md["lot_type"].values():
                        books["mbo"].clear()
                if not new["ZMMBPBook"]:
                    for books in md["lot_type"].values():
                        books["mbp"]["bids"].clear()
                        books["mbp"]["asks"].clear()
            else:
                if not new["AggregatedBook"]:
                    for books in md["lot_type"].values():
                        books["mbp"]["bids"].clear()
                        books["mbp"]["asks"].clear()
            L.debug(f'after:\n{md}')
        return res


    async def update_ins_status_sub(self, ident, msg):
        body = msg["Body"]
        srt = body["SubscriptionRequestType"]
        sub = ParameterlessSubscription(body)
        ins_id = body["ZMInstrumentID"]
        sub_key = ins_id
        subs_by_key = self._ins_status_subs[sub_key]
        ident_key = tuple(ident)
        cd_subs = self._client_data[ident_key]["ins_status_subs"]
        async def us_update_afn(new_sub):
            body = self._update_upstream_sub_base(new_sub)
            body["ZMInstrumentID"] = ins_id
            res = await g.ctl.send_recv_command(
                    fix.MsgType.SecurityStatusRequest,
                    body=body,
                    check_error=True)
            return res
        res, old, new = (await self._update_sub(
                sub=sub,
                srt=srt,
                subs_by_key=subs_by_key,
                sub_key=sub_key,
                ident_key=ident_key,
                cd_subs=cd_subs,
                res_msg_type=fix.MsgType.ZMSecurityStatusRequestResponse,
                us_update_afn=us_update_afn,
                subs_merged=self.ins_status_subs_merged))
        if new is None:
            g.ins_data[ins_id]["status"] = None
        return res


    async def update_ins_def_sub(self, ident, msg):
        body = msg["Body"]
        srt = body["SubscriptionRequestType"]
        sub = ParameterlessSubscription(body)
        ins_id = body["ZMInstrumentID"]
        sub_key = ins_id
        subs_by_key = self._ins_def_subs[sub_key]
        ident_key = tuple(ident)
        cd_subs = self._client_data[ident_key]["ins_def_subs"]
        async def us_update_afn(new_sub):
            body = self._update_upstream_sub_base(new_sub)
            body["ZMInstrumentID"] = ins_id
            res = await g.ctl.send_recv_command(
                    fix.MsgType.SecurityDefinitionRequest,
                    body=body,
                    check_error=True)
            return res
        res, old, new = (await self._update_sub(
                sub=sub,
                srt=srt,
                subs_by_key=subs_by_key,
                sub_key=sub_key,
                ident_key=ident_key,
                cd_subs=cd_subs,
                res_msg_type=fix.MsgType.ZMSecurityDefinitionRequestResponse,
                us_update_afn=us_update_afn,
                subs_merged=self.ins_def_subs_merged))
        ins_data = g.ins_data[ins_id]
        if new is None:
            ins_data["def"] = None
            ins_data["def_tt"] = None
        return res


    def update_sec_list_sub(self, ident, msg):
        body = deepcopy(msg["Body"])
        ident_key = tuple(ident)
        cd = self._client_data[ident_key]
        req_id = body["ZMReqID"]
        srt = body["SubscriptionRequestType"]
        if srt == fix.SubscriptionRequestType.SnapshotAndUpdates:
            if req_id in cd["sec_list_subs"]:
                raise RejectException(
                        "subscription with duplicate ZMReqID exists")
            cd["sec_list_subs"][req_id] = body
        elif srt == fix.SubscriptionRequestType.Unsubscribe:
            if req_id not in cd["sec_list_subs"]:
                raise RejectException(
                        "subscription not found")
            del cd["sec_list_subs"][req_id]


    # async def update_ins_md_sub(self, ident, msg):

    #     msg = deepcopy(msg)
    #     ident_key = tuple(ident)
    #     id_sub = msg["Body"]
    #     ins_id = id_sub.pop("ZMInstrumentID")
    #     market_id = id_sub.pop("MarketID", None)
    #     market_seg_id = id_sub.pop("MarketSegmentID", None)
    #     sub_key = (ins_id, market_id, market_seg_id)
    #     # sub_key = (id_sub.pop("ZMInstrumentID"),
    #     #            id_sub.pop("MarketID", None),
    #     #            id_sub.pop("MarketSegmentID", None))
    #     ts_now = get_timestamp()
    #     srt = id_sub.pop("SubscriptionRequestType")
    #     subs = self._ins_md_subs[sub_key]

    #     prev_id_sub_entry = None
    #     for k in subs.keys():
    #         _, idt = k
    #         if idt == ident_key:
    #             prev_id_sub_entry = (k, subs[k])
    #             break

    #     if srt == fix.SubscriptionRequestType.Unsubscribe:
    #         if prev_id_sub_entry is None:
    #             raise RejectException("no subscription found")
    #         old_sub, _ = self._merge_all_md_subs(subs)
    #         del subs[prev_id_sub_entry[0]]
    #         new_sub, _ = self._merge_all_md_subs(subs)
    #         if new_sub != old_sub:
    #             try:
    #                 res = await self._update_upstream_md_sub(
    #                         ins_id, market_id, market_seg_id, new_sub)
    #             except RejectException:
    #                 if prev_id_sub_entry:
    #                     subs[prev_id_sub_entry[0]] = prev_id_sub_entry[1]
    #                 raise
    #             if new_sub is None:
    #                 if market_id is None and market_seg_id is None:
    #                     # all markets
    #                     g.ins_data[ins_id]["markets"].clear()
    #                 else:
    #                     # specific market
    #                     g.ins_data[ins_id]["markets"].pop(market_key, None)
    #                 
    #         else:
    #             res = {}
    #             res["Header"] = \
    #                     {"MsgType": fix.MsgType.ZMMarketDataRequestResponse}
    #             res["Body"] = {}
    #             res["Body"]["Text"] = "no change in upstream subscription"
    #         del self._client_data[ident_key]["ins_md_subs"][sub_key]

    #     else:
    #         old_sub, ts_contained = self._merge_all_md_subs(
    #                 subs, id_sub)
    #         if ts_contained is None:
    #             ts_contained = ts_now
    #         if prev_id_sub_entry:
    #             del subs[prev_id_sub_entry[0]]
    #         subs[(ts_now, ident_key)] = id_sub
    #         new_sub, _ = self._merge_all_md_subs(subs)
    #         if new_sub != old_sub:
    #             try:
    #                 res = await self._update_upstream_md_sub(
    #                         ins_id, market_id, market_seg_id, new_sub)
    #             except RejectException:
    #                 if prev_id_sub_entry:
    #                     subs[prev_id_sub_entry[0]] = prev_id_sub_entry[1]
    #                 raise
    #             # g.ins_data[ins_id]["markets"][market_key]["subscribed"] = True
    #         else:
    #             res = {}
    #             res["Header"] = \
    #                     {"MsgType": fix.MsgType.ZMMarketDataRequestResponse}
    #             res["Body"] = {}
    #             res["Body"]["Text"] = "no change in upstream subscription"
    #         res["Body"]["_TimeSubscribed"] = ts_contained
    #         self._client_data[ident_key]["ins_md_subs"][sub_key] = id_sub

    #     return res

    # async def _update_simple_sub(self,
    #                              ident,
    #                              msg,
    #                              subs,
    #                              cd_subs,
    #                              upd_upstream,
    #                              res_msg_type):
    #     msg = deepcopy(msg)
    #     ident_key = tuple(ident)
    #     body = msg["Body"]
    #     ins_id = body["ZMInstrumentID"]
    #     srt = body["SubscriptionRequestType"]
    #     ts_now = get_timestamp()

    #     if srt == fix.SubscriptionRequestType.Unsubscribe:
    #         if ident_key not in subs:
    #             raise RejectException("no subscription found")
    #         old_ts = subs.pop(ident_key)
    #         if not subs:
    #             try:
    #                 res = await upd_upstream(ins_id, False)
    #             except RejecException:
    #                 subs[ident_key] = old_ts
    #                 raise
    #         del cd_subs.remove(ins_id)

    #     else:
    #         if ident_key in subs:
    #             raise RejectException("already subscribed")
    #         if len(subs) > 0:
    #             need_subscription = False
    #             ts_subscribed = max(subs.values())
    #         else:
    #             need_subscription = True
    #         subs[ident_key] = ts_now
    #         if need_subscription:
    #             try:
    #                 res = await upd_upstream(ins_id, True)
    #             except RejectException:
    #                 del subs[ident_key]
    #                 raise
    #         else:
    #             res = {}
    #             res["Header"] = {"MsgType": res_msg_type}
    #             res["Body"] = {}
    #             res["Body"]["Tetx"] = "no change in upstream subscription"
    #             res["Body"]["_TimeSubscribed"] = ts_subscribed
    #         cd_subs.add(ins_id)

    #     return res


    # async def update_ins_def_sub(self, ident, msg):
    #     return await self._update_simple_sub(
    #             ident,
    #             msg,
    #             self._ins_def_subs[msg["Body"]["ZMInstrumentID"]],
    #             self._client_data[tuple(ident)]["ins_def_subs"],
    #             self._update_upstream_ins_def_sub,
    #             fix.MsgType.ZMSecurityDefinitionRequestResponse)


    # async def update_ins_status_sub(self, ident, msg):
    #     return await self._update_simple_sub(
    #             ident,
    #             msg,
    #             self._ins_status_subs[msg["Body"]["ZMInstrumentID"]],
    #             self._client_data[tuple(ident)]["ins_status_subs"],
    #             self._update_upstream_ins_status_sub,
    #             fix.MsgType.ZMSecurityStatusRequestResponse)


    async def update_market_def_sub(self, ident, msg):
        raise RejectException("not yet implemented")


    async def update_ts_status_sub(self, ident, msg):
        raise RejectException("not yet implemented")


    async def _remove_client(self, ident_key):
        # Remove client from internal data structures.
        # If diff detected on any subscriptions, modify upstream subsription.
        client_data = self._client_data[ident_key]
        for (ins_id, m_id, ts_id) in list(client_data["ins_md_subs"].keys()):
            msg = {}
            msg["Body"] = body = {}
            body["ZMInstrumentID"] = ins_id
            body["ZMMarketID"] = m_id
            body["ZMTradingSessionID"] = ts_id
            body["SubscriptionRequestType"] = \
                    fix.SubscriptionRequestType.Unsubscribe
            try:
                await self.update_ins_md_sub(ident_key, msg)
            except RejectException:
                L.exception("error when unsubscribing "
                            f"MarketDataRequest {ins_id}:")
        for ins_id in list(client_data["ins_def_subs"].keys()):
            msg = {}
            msg["Body"] = body = {}
            body["ZMInstrumentID"] = ins_id
            body["SubscriptionRequestType"] = \
                    fix.SubscriptionRequestType.Unsubscribe
            try:
                await self.update_ins_def_sub(ident_key, msg)
            except RejectException:
                L.exception("error when unsubscribing "
                            f"SecurityDefinitionRequest {ins_id}:")
        for ins_id in list(client_data["ins_status_subs"].keys()):
            msg = {}
            msg["Body"] = body = {}
            body["ZMInstrumentID"] = ins_id
            body["SubscriptionRequestType"] = \
                    fix.SubscriptionRequestType.Unsubscribe
            try:
                await self.update_ins_status_sub(ident_key, msg)
            except RejectException:
                L.exception("error when unsubscribing "
                            f"SecurityStatusRequest {ins_id}:")
        for req_id in client_data["sec_list_subs"].keys():
            body = {}
            body["ZMReqID"] = req_id
            body["SubscriptionRequestType"] = \
                    fix.SubscriptionRequestType.Unsubscribe
            try:
                await g.ctl.send_recv_command(
                        fix.MsgType.SecurityListRequest,
                        body=body,
                        check_error=True)
            except RejectException:
                L.exception("error when unsubscribing "
                            f"SecurityListRequest {req_id}:")

        del self._client_data[ident_key]


    def restore_health(self, ident):
        ident_key = tuple(ident)
        self._client_data[ident_key]["health"] = g.timeout_heartbeats


    async def run_reaper(self):
        L.debug("reaper running ...")
        while True:
            for ident_key in list(self._client_data.keys()):
                d = self._client_data[ident_key]
                d["health"] -= 1
                if d["health"] <= 0:
                    ident_str = ident_to_str(ident_key)
                    L.info(f"client '{ident_str}' timed out, "
                           "removing subscriptions ...")
                    await self._remove_client(ident_key)
            await asyncio.sleep(g.heartbeat_interval)


###############################################################################


class MyController(MiddlewareCTL):


    def __init__(self, sock_dn, dealer, publisher=None):
        super().__init__(sock_dn, dealer, publisher)
        self._ins_def_locks = defaultdict(asyncio.Lock)


    async def _populate_ins_info3(self, ins_id):
        body_out = {
            "ZMInstrumentID": ins_id,
        }
        if fix.ZMCap.SecurityDefinitionOOBSnapshot in g.caps:
            body_out["SubscriptionRequestType"] = \
                    fix.SubscriptionRequestType.Snapshot
            return await self.send_recv_command(
                    fix.MsgType.SecurityDefinitionRequest,
                    body=body_out,
                    check_error=True)
        body_out["SubscriptionRequestType"] = \
                fix.SubscriptionRequestType.SnapshotAndUpdates
        sock = g.ctx.socket(zmq.SUB)
        sock.connect(INTERNAL_PUB_ADDR)
        poller = zmq.asyncio.Poller()
        poller.register(sock)
        timeout = g.feats.get("SecurityDefinitionSnapshotWaitTime", 5) * 1000
        topic = f"insdef {ins_id}\0".encode()
        sock.subscribe(topic)
        exception = None
        async def sub_afn():
            nonlocal exception
            try:
                await self.send_recv_command(
                        fix.MsgType.SecurityDefinitionRequest,
                        body=body_out,
                        check_error=True)
            except RejectException as e:
                # signal failure
                exception = e
                await g.sock_internal_pub.send_multipart([topic, b""])
        create_task(sub_afn())
        if not (await poller.poll(timeout)):
            raise RejectException("timed out waiting for SecurityDefinition")
        res = (await sock.recv_multipart())[-1]
        L.debug(f"pop_ins_info3 finished: {ins_id}")
        if exception:
            raise exception
        res = json.loads(res.decode())
        body_out["SubscriptionRequestType"] = \
                fix.SubscriptionRequestType.Unsubscribe
        await self.send_recv_command(
                fix.MsgType.SecurityDefinitionRequest,
                body=body_out,
                check_error=True)
        return res


    async def _populate_ins_info2(self, ins_id):
        res = await self._populate_ins_info3(ins_id)
        if not res:
            return
        d = g.ins_data[ins_id]
        body = res["Body"]
        min_increment = body["MinPriceIncrement"]
        num_decimals = count_num_decimals(min_increment)
        def r2i(x):
            return round(x / min_increment)
        def i2r(x):
            return round(x * min_increment, num_decimals)
        d["r2i"] = r2i
        d["i2r"] = i2r
        # d["max_mbp_levels"] = body.get("ZMMaxMBPLevels")
        # if not fix.ZMCap.MDMBPExplicitDelete and not d["max_mbp_levels"]:
        #     L.critical("When MDMBPExplicitDelete is not defined connector "
        #                "must provide ZMMaxMBPLevels to "
        #                "SecurityDefinitionRequest")
        #     sys.exit(1)


    async def _populate_ins_info(self, ins_id):
        async with self._ins_def_locks[ins_id]:
            L.debug(f"pop_ins_info: {ins_id}")
            return await self._populate_ins_info2(ins_id)
    

    def _gen_md_snapshot_from_cache(self, msg):
        body = msg["Body"]
        ins_id = body["ZMInstrumentID"]
        market_id = body.get("ZMMarketID")
        ts_id = body.get("ZMTradingSessionID")
        market_key = (market_id, ts_id)
        res = {}
        res["ZMInstrumentID"] = ins_id
        if market_id is not None:
            res["ZMMarketID"] = market_id
        if ts_id is not None:
            res["ZMTradingSessionID"] = ts_id
        res["NoMDEntries"] = group = []
        ins_data = g.ins_data[ins_id]
        # if market_key not in ins_data["markets"]:
        #     raise RejectException("no data")
        i2r = ins_data["i2r"]
        md = ins_data["markets"][market_key]
        group += md["generic_md"].values()
        # if fix.ZMCap.MDMBPPlusMBO in g.caps:
        #     agg_book = False
        # else:
        #     agg_book = body.get("AggregatedBook", True)
        for books in md["lot_type"].values():
            # if agg_book or body.get("ZMMBPBook"):
            for i, lvl in enumerate(books["mbp"]["bids"].values()):
                for d in lvl.values():
                    if d is not None:
                        d["MDPriceLevel"] = i + 1
                        group.append(d)
            for i, lvl in enumerate(books["mbp"]["asks"].values()):
                for d in lvl.values():
                    if d is not None:
                        d["MDPriceLevel"] = i + 1
                        group.append(d)
            # if not agg_book or body.get("ZMMBOBook"):
            for d in books["mbo"].values():
                group.append(d)
        return res


    def _gen_ins_status_snapshot_from_cache(self, msg):
        body = msg["Body"]
        ins_id = body["ZMInstrumentID"]
        if not g.ins_data[ins_id]["status"]:
            raise RejectException("no data",
                                  fix.ZMRejectReason.NoData,
                                  error_condition=False)
        return g.ins_data[ins_id]["status"]


    def _gen_ins_def_snapshot_from_cache(self, msg):
        body = msg["Body"]
        ins_id = body["ZMInstrumentID"]
        ins_data = g.ins_data[ins_id]
        res = ins_data["def"]
        if not res:
            raise RejectException("no data",
                                  fix.ZMRejectReason.NoData,
                                  error_condition=False)
        res = deepcopy(res)
        tt = ins_data["def_tt"]
        if tt:
            res["TransactTime"] = tt
        return res


    async def _publish_snapshot_base(self, msg, ret_msg_type, snapshot_fn):
        body = snapshot_fn(msg)
        rid = msg["Body"].get("ZMReqID")
        if rid is not None:
            body["ZMReqID"] = rid
        await g.pub.publish(ret_msg_type,
                            body=body,
                            topic=rid.encode()+b"\0",
                            no_seq_num=True)


    # async def _publish_md_snapshot(self, msg):
    #     await self._publish_snapshot_base(
    #             msg,
    #             fix.MsgType.MarketDataSnapshotFullRefresh,
    #             self._gen_md_snapshot_from_cache)


    # async def _publish_ins_status_snapshot(self, msg):
    #     await self._publish_snapshot_base(
    #             msg,
    #             fix.MsgType.SecurityStatus,
    #             self._gen_ins_status_snapshot_from_cache)


        # body = self._gen_md_snapshot_from_cache(msg)
        # # body["RefreshIndicator"] = False
        # rid = msg["Body"].get("ZMReqID")
        # if rid is not None:
        #     body["ZMReqID"] = rid
        # await g.pub.publish(fix.MsgType.MarketDataSnapshotFullRefresh,
        #                     body=body,
        #                     topic=rid.encode()+b"\0",
        #                     no_seq_num=True)




    # async def _gen_ins_def_snapshot_from_cache(self, msg):
    #     body = msg["Body"]
    #     ins_id = body["ZMInstrumentID"]
    #     res = g.ins_defs.get(ins_id)
    #     if not res:
    #         raise RejectException("no data")
    #     return res


    async def _publish_ins_def_snapshot(self, msg):
        body = self._gen_ins_def_snapshot_from_cache(msg)
        body["RefreshIndicator"] = False
        rid = msg["Body"].get("ZMReqID")
        if rid is not None:
            body["ZMReqID"] = rid
        await g.pub.publish(fix.MsgType.SecurityDefinition, body=body)


    #async def _publish_security_list_snapshot(self, msg):
    #    body = 



    async def _sub_request_base(self,
                                ident,
                                msg_raw,
                                msg,
                                res_msg_type,
                                res_snap_msg_type,
                                oob_snap_cap,
                                gen_snap_fn,
                                update_sub_afn):
        body = msg["Body"]
        srt = body["SubscriptionRequestType"]
        if srt == fix.SubscriptionRequestType.Snapshot:
            send_to_pub = body["ZMSendToPub"]
            if not send_to_pub and oob_snap_cap in g.caps:
                res = await self._dealer.send_recv_msg(msg_raw, ident=ident)
                return res[-1]
            else:
                res = {}
                res["Header"] = header = {}
                header["MsgType"] = res_msg_type
                if send_to_pub:
                    await self._publish_snapshot_base(
                            msg,
                            res_snap_msg_type,
                            gen_snap_fn)
                    res["Body"] = {"Text": "snapshot sent to pub"}
                    return res
                res["Body"] = gen_snap_fn(msg)
                return res
        return await update_sub_afn(ident, msg)


    async def MarketDataRequest(self, ident, msg_raw, msg):
        ins_id = msg["Body"]["ZMInstrumentID"]
        if g.ins_data[ins_id]["r2i"] is None:
            await self._populate_ins_info(ins_id)
        return await self._sub_request_base(
                ident,
                msg_raw,
                msg,
                fix.MsgType.ZMMarketDataRequestResponse,
                fix.MsgType.MarketDataSnapshotFullRefresh,
                fix.ZMCap.MDOOBSnapshot,
                self._gen_md_snapshot_from_cache,
                g.submgr.update_ins_md_sub)


    async def SecurityStatusRequest(self, ident, msg_raw, msg):
        return await self._sub_request_base(
                ident,
                msg_raw,
                msg,
                fix.MsgType.ZMSecurityStatusRequestResponse,
                fix.MsgType.SecurityStatus,
                fix.ZMCap.SecurityStatusOOBSnapshot,
                self._gen_ins_status_snapshot_from_cache,
                g.submgr.update_ins_status_sub)


    async def SecurityDefinitionRequest(self, ident, msg_raw, msg):
        ins_id = msg["Body"]["ZMInstrumentID"]
        async with self._ins_def_locks[ins_id]:
            return await self._sub_request_base(
                    ident,
                    msg_raw,
                    msg,
                    fix.MsgType.ZMSecurityStatusRequestResponse,
                    fix.MsgType.SecurityDefinition,
                    fix.ZMCap.SecurityDefinitionOOBSnapshot,
                    self._gen_ins_def_snapshot_from_cache,
                    g.submgr.update_ins_def_sub)



    async def SecurityListRequest(self, ident, msg_raw, msg):
        body = msg["Body"]
        srt = body["SubscriptionRequestType"]
        if srt != fix.SubscriptionRequestType.Snapshot:
            g.submgr.update_sec_list_sub(ident, msg)
        res = await self._dealer.send_recv_msg(msg_raw, ident=ident)
        return res[-1]
            

    # async def SecurityListRequest(self, ident, msg_raw, msg):
    #     body = msg["Body"]
    #     srt = body["SubscriptionRequestType"]
    #     if srt == fix.SubscriptionRequestType.Snapshot:
    #         if fix.ZMCap.SecurityDefinitionOOBSnapshot in g.caps:
    #             res = await self._dealer.send_recv_msg(msg_raw, ident=ident)
    #             return res[-1]
    #         else:
    #             raise RejectException("not yet implemented")
    #     return await g.submgr.update_sec_list_sub(ident, msg)


    # async def MarketDefinitionRequest(self, ident, msg_raw, msg):
    #     body = msg["Body"]
    #     srt = body["SubscriptionRequestType"]
    #     if srt == fix.SubscriptionRequestType.Snapshot:
    #         if fix.ZMCap.MarketDefinitionOOBSnapshot in g.caps:
    #             res = await self._dealer.send_recv_msg(msg_raw, ident=ident)
    #             return res[-1]
    #         else:
    #             raise RejectException("not yet implemented")
    #     return await g.submgr.update_market_def_sub(ident, msg)


    # async def TradingSessionStatusRequest(self, ident, msg_raw, msg):
    #     body = msg["Body"]
    #     srt = body["SubscriptionRequestType"]
    #     if srt == fix.SubscriptionRequestType.Snapshot:
    #         if fix.ZMCap.TradingSessionStatusOOBSnapshot in g.caps:
    #             res = await self._dealer.send_recv_msg(msg_raw, ident=ident)
    #             return res[-1]
    #         else:
    #             raise RejectException("not yet implemented")
    #     return await g.submgr.update_ts_status_sub(ident, msg)


    async def ZMGetConnectorFeatures(self, ident, msg_raw, msg):
        # res = await self._dealer.send_recv_msg(msg_raw, ident=ident)
        # res = json.loads(res[-1])
        # feats = b64decode(res["Body"]["ZMConnectorFeatures"].encode())
        # feats = json.loads(feats.decode())
        feats = deepcopy(g.feats)
        feats["HeartbeatInterval"] = g.heartbeat_interval
        feats = b64encode(json.dumps(feats).encode()).decode()
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMGetConnectorFeaturesResponse
        res["Body"] = {}
        res["Body"]["ZMConnectorFeatures"] = feats
        return res


    async def TestRequest(self, ident, msg_raw, msg):
        body = msg["Body"]
        res = {}
        res["Header"] = {"MsgType": fix.MsgType.Heartbeat}
        tr_id = body.get("TestReqID")
        res["Body"] = {}
        if tr_id:
            res["Body"]["TestReqID"] = tr_id
        return res


    async def ZMGetValidReqID(self, ident, msg_raw, msg):
        res = {}
        res["Header"] = {"MsgType": fix.MsgType.ZMGetValidReqIDResponse}
        res["Body"] = body = {}
        # Can be used as a unique prefix for any subsequent ZMReqIDs.
        body["ZMReqID"] = "{} ".format(g.next_req_id_prefix)
        g.next_req_id_prefix += 1
        return res

            
    async def _handle_msg_2(self, ident, msg_raw, msg, msg_type):
        g.submgr.restore_health(ident)
        return await super()._handle_msg_2(ident, msg_raw, msg, msg_type)


###############################################################################

# IMPORTANT NOTE:
# Handling New, Change and Overlay separately would a lot of overhead and
# complexity. The only benefit from that would be sanity check that
# should be done in a downstream module instead. Would need to keep track
# of current subscription and cease updating any data structures that are
# not subscribed for. Also upon reducing subscription level the structures
# that are no longer populated should be cleared to prevent errors on
# possible subsequent activations.


def update_mbp_from_mbo(ins_id, market_key, lot_type, new_data, old_data):
    # TODO: write this logic
    L.error("[update_mbp_from_mbo] not yet implemented")


def update_mbo(ins_id, market_key, lot_type, data):
    book = g.ins_data[ins_id]["markets"][market_key]["lot_type"][lot_type]["mbo"]
    old_data = None
    et = data["MDEntryType"]
    L.debug("[update_mbo] ins_id={}, len={}, et={}"
            .format(ins_id, len(book), et))
    if et == fix.MDEntryType.EmptyBook:
        book.clear()
    else:
        order_id = data["OrderID"]
        ua = data.pop("MDUpdateAction")
        if ua == fix.MDUpdateAction.New:
            if fix.ZMCap.MDSaneMBO in g.caps and order_id in data:
                L.warning("[update_mbo] MDUpdateAction New replacing old "
                          f"order: ins_id={ins_id} order_id={order_id}")
            book[order_id] = data
        elif ua == fix.MDUpdateAction.Change:
            old_data = book.pop(order_id, None)
            if fix.ZMCap.MDSaneMBO in g.caps and not old_data:
                L.warning("[update_mbo] tried to change non-existent "
                          f"order: ins_id={ins_id} order_id={order_id}")
            book[order_id] = data
        elif ua == fix.MDUpdateAction.Delete:
            if book.pop(order_id, None) is None \
                    and fix.ZMCap.MDSaneMBO in g.caps:
                L.warning("[update_mbo] tried to delete non-existent "
                          f"order: ins_id={ins_id} order_id={order_id}")
        else:
            L.error(f"[update_mbo] unrecognized MDUpdateAction: {ua}")
            return
    if not fix.ZMCap.MDMBPPlusMBO in g.caps:
        update_mbp_from_mbo(ins_id, market_key, lot_type, data, old_data)


# Will not emit explicit delete messages here, it's responsibility of
# sessionizer. This is to minimize load on this module.

def update_mbp(ins_id, market_key, lot_type, data):
    ins_data = g.ins_data[ins_id]
    r2i = ins_data["r2i"]
    book = ins_data["markets"][market_key]["lot_type"][lot_type]["mbp"]
    et = data["MDEntryType"]
    if et == fix.MDEntryType.EmptyBook:
        book["bids"].clear()
        book["asks"].clear()
        return
    try:
        ua = data.pop("MDUpdateAction")
    except:
        import ipdb; ipdb.set_trace()
    if et in (fix.MDEntryType.Bid, fix.MDEntryType.SimulatedBuyPrice):
        side = book["bids"]
    elif et in (fix.MDEntryType.Offer, fix.MDEntryType.SimulatedSellPrice):
        side = book["asks"]
    else:
        L.critical(f"unexpected MDEntryType: {et}")
        sys.exit(1)
    if et in (fix.MDEntryType.Bid, fix.MDEntryType.Offer):
        lvl_name = "normal"
    else:
        lvl_name = "implied"
    price = data["MDEntryPx"]
    size = data["MDEntrySize"]
    L.debug("[update_mbp] ins_id={} len={}, et={}, ua={}, price={}, size={}"
            .format(ins_id, len(side), et, ua, price, size))
    i_price = r2i(price)
    if ua == fix.MDUpdateAction.Delete or \
            (ua == fix.MDUpdateAction.Overlay and size == 0):
        try:
            pos = side.index(i_price)
        except ValueError:
            L.warning("[update_mbp] tried to delete non-existing "
                      f"level at price: ins={ins_id} price={price}")
            return
        lvls = side[i_price]
        if not lvls.pop(lvl_name, None):
            L.warning("[update_mbp] tried to delete non-existing "
                      f"level at price: ins={ins_id} price={price} "
                      f"lvl_name={lvl_name}")
            return
        if not lvls:
            # print(f"popped {price}!")
            del side[i_price]
        return
    if size == 0:
        L.warning("[update_mbp] size 0 detected on a non-removing entry: "
                  f"ins_id={ins_id} price={price}")
    # if ua in (fix.MDUpdateAction.New,
    #           fix.MDUpdateAction.Change,
    #           fix.MDUpdateAction.Overlay):
    #     if i_price not in side:
    #         side[i_price] = {}
    #     side[i_price][lvl_name] = data
    #     return
    if ua == fix.MDUpdateAction.Change:
        try:
            lvls = side[i_price]
        except KeyError:
            L.warning("[update_mbp] tried to change non-existing"
                      f"level at price: ins_id={ins_id} price={price}")
            return
        lvls[lvl_name] = data
        return
    if ua == fix.MDUpdateAction.New:
        # if i_price in side:
        #     L.warning("[update_mbp] UpdateAction.New overwriting existing "
        #               f"level: ins_id={ins_id} price={price}")
        #     return
        # side[i_price] = lvls = {}
        if i_price not in side:
            side[i_price] = lvls = {}
        lvls[lvl_name] = data
        return
    if ua == fix.MDUpdateAction.Overlay:
        if i_price not in side:
            side[i_price] = {}
        side[i_price][lvl_name] = data
        return
    L.error(f"[update_mbp] unrecognized MDUpdateAction: {ua}")


def update_generic_md(ins_id, market_key, data):
    ins_data = g.ins_data[ins_id]
    gen_md = ins_data["markets"][market_key]["generic_md"]
    et = data["MDEntryType"]
    # if et in (fix.MDEntryType.Bid,
    #           fix.MDEntryType.Offer,
    #           fix.MDEntryType.SimulatedBuyPrice,
    #           fix.MDEntryType.SimulatedSellPrice):
    #     data["_IsTopOfBook"] = True
    try:
        ua = data.pop("MDUpdateAction")
    except:
        import ipdb; ipdb.set_trace()
    L.debug("[gen_md] ins_id={} len={} et={}, ua={}"
            .format(ins_id, len(gen_md), et, ua))
    if ua == fix.MDUpdateAction.Delete:
        if not gen_md.pop(et, None):
            L.warning("[update_generic_md] tried to delete non-existing "
                      f"entry: ins_id={ins_id} et={et}")
        return
    gen_md[et] = data


def handle_md_inc_refresh_entry(market_key, ins_id, data, sub=None):
    lot_type = data.get("LotType")
    ins_id = data.pop("ZMInstrumentID", ins_id)
    if not sub:
        sub = g.submgr.ins_md_subs_merged.get((ins_id,) + market_key)
        if sub is None:
            return ins_id
    # if not g.ins_data[ins_id]["markets"][market_key]["subscribed"]:
    #     return ins_id
    if "*" not in sub["NoMDEntryTypes"] \
            and data["MDEntryType"] not in sub["NoMDEntryTypes"]:
        return ins_id
    if data["MDEntryType"] in (fix.MDEntryType.Bid,
                               fix.MDEntryType.Offer,
                               fix.MDEntryType.EmptyBook):
        if "OrderID" in data \
                or data.get("MDBookType") == fix.MDBookType.OrderDepth:
            update_mbo(ins_id, market_key, lot_type, data)
        else:
            if data.get("MDPriceLevel") == 1:
                update_generic_md(ins_id, market_key, data)
            else:
                update_mbp(ins_id, market_key, lot_type, data)
    elif data["MDEntryType"] in (fix.MDEntryType.SimulatedBuyPrice,
                                 fix.MDEntryType.SimulatedSellPrice):
        if data.get("MDPriceLevel") == 1:
            update_generic_md(ins_id, market_key, data)
        else:
            update_mbp(ins_id, market_key, lot_type, data)
    else:
        if lot_type:
            L.warning("[handle_md_inc_refresh_entry] unexpected lot_type when"
                      "updating generic md:\{}".format(pformat(data)))
        update_generic_md(ins_id, market_key, data)
    return ins_id


def handle_md_inc_refresh(header, body):
    market_key = (body.get("ZMMarketID"), body.get("ZMTradingSessionID"))
    ins_id = None
    for x in body["NoMDEntries"]:
        x = deepcopy(x)
        ins_id = handle_md_inc_refresh_entry(market_key, ins_id, x)
    return True


def handle_md_snapshot(header, body):
    ins_id = body["ZMInstrumentID"]
    market_key = (body.get("ZMMarketID"), body.get("ZMTradingSessionID"))
    sub = g.submgr.ins_md_subs_merged.get((ins_id,) + market_key)
    if sub is None:
        return False
    # if not g.ins_data[ins_id]["markets"][market_key]["subscribed"]:
    #     continue
    g.ins_data[ins_id]["markets"].pop(market_key, "")
    for x in body["NoMDEntries"]:
        x = deepcopy(x)
        x["MDUpdateAction"] = fix.MDUpdateAction.New
        handle_md_inc_refresh_entry(market_key, ins_id, x, sub)
    return True


def handle_security_status(header, body):
    ins_id = body["ZMInstrumentID"]
    if g.submgr.ins_status_subs_merged.get(ins_id) is None:
        return False
    body = deepcopy(body)
    g.ins_data[ins_id]["status"] = body
    return True


def handle_ts_status(header, body):
    key = body["ZMTradingSessionID"]
    if g.submgr.ts_status_subs_merged.get(key) is None:
        return False
    body = deepcopy(body)
    g.ts_status[key] = body
    return True


def handle_market_definition(header, body):
    key = body["ZMMarketID"]
    if g.submgr.market_def_subs_merged.get(key) is None:
        return False
    body = deepcopy(body)
    g.market_defs[key] = body
    return True


def handle_security_definition(header, body):
    body = deepcopy(body)
    ins_id = body["ZMInstrumentID"]
    if g.submgr.ins_def_subs_merged.get(ins_id) is None:
        return False
    ins_data = g.ins_data[ins_id]
    ins_data["def_tt"] = body.pop("TransactTime", None)
    if ins_data["def"] == body:
        return False
    ins_data["def"] = body
    return True


def handle_sub_msg_1(header, body):
    msg_type = header["MsgType"]
    if msg_type == fix.MsgType.MarketDataIncrementalRefresh:
        return handle_md_inc_refresh(header, body)
    if msg_type == fix.MsgType.MarketDataSnapshotFullRefresh:
        return handle_md_snapshot(header, body)
    if msg_type == fix.MsgType.SecurityStatus:
        return handle_security_status(header, body)
    if msg_type == fix.MsgType.TradingSessionStatus:
        return handle_ts_status(header, body)
    if msg_type == fix.MsgType.MarketDefinition:
        return handle_market_definition(header, body)
    if msg_type == fix.MsgType.SecurityDefinition:
        return handle_security_definition(header, body)


async def sub_handler():
    L.info("running sub handler ...")
    while True:
        msg_parts = await g.sock_sub.recv_multipart()
        msg = json.loads(msg_parts[-1].decode())
        try:
            header = msg["Header"]
            body = msg["Body"]
            if header["MsgType"] == fix.MsgType.SecurityDefinition:
                # In case connector does not support
                # SecurityDefinitionOOBSnapshot cap ...
                ins_id = body["ZMInstrumentID"]
                topic = f"insdef {ins_id}\0".encode()
                await g.sock_internal_pub.send_multipart(
                        [topic, msg_parts[-1]])
            if handle_sub_msg_1(header, body):
                await g.pub.publish_msg(msg)
        except Exception:
            L.exception("error handling pub message:")


###############################################################################


def parse_args():
    desc = "mdmgr middleware module"
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("ctl_addr_up",
                        help="address of the upstream ctl socket")
    parser.add_argument("ctl_addr_down",
                        help="ctl socket binding address")
    parser.add_argument("pub_addr_up",
                        help="address of the upstream pub socket")
    parser.add_argument("pub_addr_down",
                        help="pub socket binding address")
    parser.add_argument("--timeout-heartbeats", type=int, default=3,
                        help="maximum heartbeats to skip before timeout")
    parser.add_argument("--heartbeat-interval", type=float, default=60,
                        help="heartbeat interval in seconds")
    parser.add_argument("--log-level", default="INFO", help="logging level")
    args = parser.parse_args()
    g.timeout_heartbeats = args.timeout_heartbeats
    g.heartbeat_interval = args.heartbeat_interval
    try:
        args.log_level = int(args.log_level)
    except ValueError:
        pass
    return args


def setup_logging(args):
    setup_basic_logging(args.log_level)
    disable_logger("parso.python.diff")
    disable_logger("parso.cache")


def init_zmq_sockets(args):
    g.sock_deal = g.ctx.socket(zmq.DEALER)
    g.sock_deal.set(zmq.IDENTITY, b"mdmgr")
    g.sock_deal.connect(args.ctl_addr_up)
    g.dealer = ReturningDealer(g.ctx, g.sock_deal)
    g.sock_ctl = g.ctx.socket(zmq.ROUTER)
    g.sock_ctl.bind(args.ctl_addr_down)
    g.sock_sub = g.ctx.socket(zmq.SUB)
    g.sock_sub.connect(args.pub_addr_up)
    g.sock_sub.subscribe(b"")
    g.sock_pub = g.ctx.socket(zmq.PUB)
    g.sock_pub.bind(args.pub_addr_down)
    g.sock_internal_pub = g.ctx.socket(zmq.PUB)
    g.sock_internal_pub.bind(INTERNAL_PUB_ADDR)


async def send_recv_command_raw(msg_type, body=None):
    return await zmapi.utils.send_recv_command_raw(
            g.sock_deal, msg_type, body=body)


async def init_get_capabilities():
    res = await send_recv_command_raw(fix.MsgType.ZMListCapabilities)
    return res["Body"]["ZMNoCaps"]


async def init_get_connector_features():
    try:
        res = await send_recv_command_raw(fix.MsgType.ZMGetConnectorFeatures)
    except RejectException:
        L.debug("no connector features defined")
        return {}
    res = res["Body"]["ZMConnectorFeatures"]
    res = json.loads(b64decode(res.encode()).decode())
    L.debug("connector features:\n{}".format(pformat(res)))
    return res


def main():

    g.args = parse_args()
    setup_logging(g.args)
    init_zmq_sockets(g.args)
    g.caps = g.loop.run_until_complete(init_get_capabilities())
    g.feats = g.loop.run_until_complete(init_get_connector_features())
    g.submgr = SubscriptionManager()
    g.pub = Publisher(g.sock_pub)
    g.ctl = MyController(g.sock_ctl, g.dealer, publisher=g.pub)
    tasks = [
        g.ctl.run(),
        sub_handler(),
        g.submgr.run_reaper(),
    ]
    tasks = [create_task(coro_obj) for coro_obj in tasks]

    try:
        g.loop.run_until_complete(asyncio.gather(*tasks))
    except KeyboardInterrupt:
        pass

    g.ctx.destroy()

if __name__ == "__main__":
    main()
