import zmq
import zmq.auth
import zmq.asyncio
import asyncio
from asyncio import ensure_future as create_task
import sys
import logging
import json
import argparse
import itertools
import os
import stat
import numpy as np
from zmapi.zmq.utils import split_message, ident_to_str
from zmapi.logging import setup_root_logger, disable_logger
from zmapi.utils import (check_missing, delayed, get_timestamp,
                         count_num_decimals, check_if_error)
from zmapi.zmq import ReturningDealer
from zmapi.controller import MiddlewareCTL
from zmapi import fix, Publisher, Subscriber
from zmapi.exceptions import *
import uuid
from async_lru import alru_cache
from sortedcontainers import SortedDict
from collections import defaultdict, OrderedDict
from time import gmtime
from pprint import pprint, pformat
from datetime import datetime
from copy import deepcopy
from base64 import b64encode, b64decode


################################## CONSTANTS ##################################

INTERNAL_PUB_ADDR = "inproc://internal-pub"


################################### GLOBALS ###################################


L = logging.root

class GlobalState:
    pass
g = GlobalState()
g.loop = asyncio.get_event_loop()
g.ctx = zmq.asyncio.Context()
g.pub_initialized = asyncio.Event()
# key: ZMEndpoint
g.submgrs = {}
g.sub_handlers = {}
# int ZMReqID used internally only
g.init_snapshot_req_id = 0
g.req_id_prefix = None


###############################################################################


class SubHandler:

    def __init__(self, msg):
        header = msg["Header"]
        body = msg["Body"]
        self._endpoint = header["ZMEndpoint"]
        self.req_id = body["ZMReqID"]
        self._tag = "[{}-{}] ".format(self.__class__.__name__, self.req_id)
        self._sub_msg = msg
        self._snap_req_ids = set()


    async def _request_initial_snapshots(
            self,
            params_iterable,
            mutate_body_fn,
            req_msg_type):
        for params in params_iterable:
            body = {}
            req_id = "{} {}".format(
                    g.ctl.session_id, g.init_snapshot_req_id)
            g.init_snapshot_req_id += 1
            self._snap_req_ids.add(req_id)
            body["ZMReqID"] = req_id
            body["SubscriptionRequestType"] = \
                    fix.SubscriptionRequestType.Snapshot
            body["ZMSendToPub"] = True
            # if mutate_body_fn:
            mutate_body_fn(body, params)
            await g.ctl.send_recv_command(
                    req_msg_type,
                    body=body,
                    endpoint=self._endpoint)


    async def initialize(self):
        pass


    async def process_msg(self, msg):
        raise NotImplementedError("must be implemented")
        # header = msg["Header"]
        # body = msg["Body"]
        # endpoint = header.get("ZMEndpoint")
        # if endpoint != self._endpoint:
        #     return
        # L.debug(self._tag + "processing {} message ..."
        #         .format(header["MsgType"]))
        # msg["ZMReqID"] = self.req_id
        # return msg


class SecurityStatusSubHandler(SubHandler):


    async def initialize(self):
        body = self._sub_msg["Body"]
        group = body["NoRelatedSym"]
        self.instruments = set(group)
        def mutate_body_fn(body, params):
            body["ZMInstrumentID"] = params[0]
        await self._request_initial_snapshots(
                [[x] for x in self.instruments],
                mutate_body_fn,
                fix.MsgType.SecurityStatusRequest)


    async def process_msg(self, msg):
        header = msg["Header"]
        body = msg["Body"]
        endpoint = header.get("ZMEndpoint")
        if endpoint != self._endpoint:
            return
        req_id = body.get("ZMReqID")
        if req_id is not None and req_id not in self._snap_req_ids:
            return
        msg_type = header["MsgType"]
        if msg_type != fix.MsgType.SecurityStatus:
            return
        ins_id = body["ZMInstrumentID"]
        if ins_id not in self.instruments:
            return
        L.debug(self._tag + "processing {} message ..."
                .format(header["MsgType"]))
        body["ZMReqID"] = self.req_id
        return msg


class SecurityDefinitionSubHandler(SubHandler):

    
    async def initialize(self):
        body = self._sub_msg["Body"]
        self.instrument = body["ZMInstrumentID"]
        def mutate_body_fn(body, params):
            body["ZMInstrumentID"] = self.instrument
        await self._request_initial_snapshots(
                [self.instrument],
                mutate_body_fn,
                fix.MsgType.SecurityDefinitionRequest)


    async def process_msg(self, msg):
        header = msg["Header"]
        body = msg["Body"]
        endpoint = header.get("ZMEndpoint")
        if endpoint != self._endpoint:
            return
        req_id = body.get("ZMReqID")
        if req_id is not None and req_id not in self._snap_req_ids:
            return
        msg_type = header["MsgType"]
        if msg_type != fix.MsgType.SecurityDefinition:
            return
        ins_id = body["ZMInstrumentID"]
        if ins_id != self.instrument:
            return
        L.debug(f'{self._tag} processing {header["MsgType"]} message ...')
        body["ZMReqID"] = self.req_id
        return msg


class SecurityListSubHandler(SubHandler):


    async def initialize(self, us_req_id):
        body = self._sub_msg["Body"]
        # r = await g.ctl.send_recv_command(fix.MsgType.ZMGetValidReqID)
        self._ds_req_id = body["ZMReqID"]
        self._us_req_id = us_req_id
        self._prev_body = None


    async def process_msg(self, msg):
        header = msg["Header"]
        body = msg["Body"]
        endpoint = header.get("ZMEndpoint")
        if endpoint != self._endpoint:
            return
        req_id = body.get("ZMReqID")
        if req_id != self._us_req_id:
            return
        msg_type = header["MsgType"]
        if msg_type != fix.MsgType.SecurityList:
            return
        L.debug(f"{self._tag} processing {msg_type} message ...")
        cmp_body = deepcopy(msg["Body"])
        cmp_body.pop("TransactTime", None)
        if self._prev_body == cmp_body:
            return
        self._prev_body = cmp_body
        msg["Body"]["ZMReqID"] = self._ds_req_id
        return msg


class MDSubHandler(SubHandler):

    
    async def _add_data_from_ins_def(self, endpoint, ins_id):
        res = await g.ctl.get_ins_definition(self._endpoint, ins_id)
        res = res["Body"]
        ins_data = self._ins_data[ins_id]
        min_increment = res["MinPriceIncrement"]
        num_decimals = count_num_decimals(min_increment)
        def r2i(x):
            return round(x / min_increment)
        def i2r(x):
            return round(x * min_increment, num_decimals)
        ins_data["r2i"] = r2i
        ins_data["i2r"] = i2r
        ins_data["mbp_depth"] = min(self._mbp_depth,
                              res.get("ZMMaxMBPLevels", sys.maxsize))
        ins_data["max_mbp_levels"] = res.get("ZMMaxMBPLevels")
        # L.debug(f"{ins_id}:\n{pformat(ins_data)}")


    async def initialize(self):

        msg = self._sub_msg
        self._caps = await g.ctl.get_caps(self._endpoint)
        if fix.ZMCap.MDMBPIncremental not in self._caps:
            L.critical("non-incremental order books not yet supported")
            sys.exit(1)
        body = msg["Body"]
        def create_mbp_mbo():
            return {
                "mbp": {
                    # each price level has two entries: "implied" and "normal"
                    "bids": SortedDict(lambda x: -x),
                    "bids_tr": SortedDict(lambda x: -x),
                    "debug_0": [],
                    "debug_E": [],
                    "asks": SortedDict(),
                    "asks_tr": SortedDict(),
                    "debug_1": [],
                    "debug_F": [],
                },
                "mbo": {},
            }
        def create_market_dict():
            return {
                "lot_type": defaultdict(create_mbp_mbo),
                "generic_md": {},
            }
        def create_empty_ins_data_dict():
            return {
                # key (MarketID, MarketSegmentID)
                "markets": defaultdict(create_market_dict),
                "r2i": None,
                "i2r": None,
                "max_mbp_levels": None,
                "mbp_depth": None,
            }
        # key ZMInstrumentID
        self._ins_data = {}
        group = body["NoRelatedSym"]
        self.instruments = sorted(body["NoRelatedSym"])
        self.markets = set(body.get("NoMarketSegments", []))
        self.tsessions = set(body.get("NoTradingSessions", []))
        if fix.ZMCap.MDMBPPlusMBO in self._caps:
            if body.get("ZMMBOBook"):
                self._aggbook = False
            else:
                self._aggbook = True
        else:
            self._aggbook = body.get("AggregatedBook", True)
        self._ets = body["NoMDEntryTypes"]
        if "*" in self._ets:
            self._all_ets = True
        else:
            self._all_ets = False
        self._mbp_depth = body["MarketDepth"]
        # self._trading_sessions = set()
        # for d in body.get("NoTradingSessions", []):
        #     self._trading_sessions.add((d["TradingSessionID"],
        #                                 d.get("TradingSessionSubID", None)))
        if self._mbp_depth == 0:
            self._mbp_depth = sys.maxsize
        if body.get("MDImplicitDelete"):
            self._implicit_delete = True
            raise NotImplementedError("MDImplicitDelete not yet implemented")
        else:
            self._implicit_delete = False
        self._md_upd_type = body.get("MDUpdateType",
                                     fix.MDUpdateType.FullRefresh)
        if self._md_upd_type != fix.MDUpdateType.FullRefresh:
            raise NotImplementedError("MDUpdateType not yet implemented")
        self._us_implicit_delete = \
                fix.ZMCap.MDMBPExplicitDelete not in self._caps
        self._generate_bbo = fix.ZMCap.MDBBO not in self._caps
        for ins_id in self.instruments:
            self._ins_data[ins_id] = create_empty_ins_data_dict()

        for ins_id in self._ins_data:
            await self._add_data_from_ins_def(self._endpoint, ins_id)

        instruments = deepcopy(self.instruments)
        markets = deepcopy(self.markets)
        if not markets:
            markets = [None]
        tsessions = deepcopy(self.tsessions)
        if not tsessions:
            tsessions = [None]
        def mutate_body_fn(body, params):
            ins_id, market_id, ts_id = params
            body["ZMInstrumentID"] = ins_id
            if market_id:
                body["ZMMarketID"] = market
            if ts_id:
                body["ZMTradingSessionID"] = ts_id
        await self._request_initial_snapshots(
                itertools.product(instruments, markets, tsessions),
                mutate_body_fn,
                fix.MsgType.MarketDataRequest)

        # for ins_id in self._ins_data:
        #     body = {}
        #     req_id = "{} {}".format(
        #             g.ctl.session_id, g.md_snapshot_req_id)
        #     g.md_snapshot_req_id += 1
        #     self._snap_req_ids.add(req_id)
        #     body["ZMReqID"] = req_id
        #     body["SubscriptionRequestType"] = \
        #             fix.SubscriptionRequestType.Snapshot
        #     body["ZMInstrumentID"] = ins_id
        #     body["ZMSendToPub"] = True
        #     await g.ctl.send_recv_command(
        #             fix.MsgType.MarketDataRequest,
        #             body=body,
        #             endpoint=self._endpoint)


    def _update_mbp_from_mbo(self, ins_id, market_key, lot_type,
                             new_data, old_data, mbp_entries):
        raise NotImplementedError()


    def _update_mbo(self, ins_id, market_key, lot_type,
                    et, entry, mbp_entries):
        if fix.ZMCap.MDMBPPlusMBO in self._caps and self._aggbook:
            return []
        ins_data = self._ins_data[ins_id]
        book = ins_data["markets"][market_key]["lot_type"][lot_type]["mbo"]
        old_entry = None
        order_id = None
        L.debug(self._tag + "[update_mbo] ins_id={}, len={}, et={}".format(
                ins_id, len(book), et))
        if et == fix.MDEntryType.EmptyBook:
            book.clear()
        else:
            ua = entry["MDUpdateAction"]
            order_id = entry["OrderID"]
            if ua == fix.MDUpdateAction.New:
                book[order_id] = entry
            elif ua == fix.MDUpdateAction.Change:
                old_entry = book.pop(order_id, None)
                if not old_entry:
                    L.warning(self._tag + "[update_mbo] tried to change "
                              f"non-existent order id: ins_id={ins_id} "
                              f"order_id={order_id}")
                book[order_id] = entry
            elif ua == fix.MDUpdateAction.Delete:
                if book.pop(order_id, None) is None:
                    L.warning(self._tag + "[update_mbo] tried to delete "
                              f"non-existent order id: ins_id={ins_id} "
                              f"order_id={order_id}")
            else:
                L.warning(self._tag + "[update_mbo] unrecognized "
                          f"UpdateAction: {ua}")
                return []
        if self._aggbook:
            self._update_mbp_from_mbo(
                    ins_id, market_key, lot_type,
                    entry, old_entry, mbp_entries)
        else:
            return [entry]


    def _update_mbp(self, ins_id, market_key, lot_type,
                    et, entry, mbp_entries):
        # TODO: don't add to mbp_entries too big pricelevels for optimization
        ins_data = self._ins_data[ins_id]
        mbp_depth = ins_data["mbp_depth"]
        r2i = ins_data["r2i"]
        book = ins_data["markets"][market_key]["lot_type"][lot_type]["mbp"]
        sel_entries = mbp_entries[(ins_id, lot_type)]
        # sel_entries = mbp_entries[ins_id][(market_key, lot_type)]
        if et == fix.MDEntryType.EmptyBook:
            book["bids"].clear()
            book["bids_tr"].clear()
            book["asks"].clear()
            book["asks_tr"].clear()
            sel_entries.append(entry)
            return
        ua = entry["MDUpdateAction"]
        if et in (fix.MDEntryType.Bid, fix.MDEntryType.SimulatedBuyPrice):
            side = book["bids"]
            side_tr = book["bids_tr"]
        elif et in (fix.MDEntryType.Offer, fix.MDEntryType.SimulatedSellPrice):
            side = book["asks"]
            side_tr = book["asks_tr"]
        else:
            raise RuntimeError(f"unexpected MDEntryType: {et}")
        if et in (fix.MDEntryType.Bid, fix.MDEntryType.Offer):
            lvl_name = "normal"
        else:
            lvl_name = "implied"
        price = entry["MDEntryPx"]
        i_price = r2i(price)
        size = entry["MDEntrySize"]
        L.debug(self._tag + "[update_mbp] ins_id={}, len={}, et={}, ua={}, "
                "price={}, i_price={}, size={}".format(
                ins_id, len(side), et, ua, price, i_price, size))
        add_entry = False
        if ua == fix.MDUpdateAction.Delete or \
                (ua == fix.MDUpdateAction.Overlay and size == 0):
            if i_price not in side:
                L.warning(self._tag + "[update_mbp] tried to delete "
                          f"non-existing level at price: ins={ins_id} "
                          f"price={price}")
                return
            # try:
            #     pos = side.index(i_price) + 1
            # except ValueError:
            #     L.warning(self._tag + "[update_mbp] tried to delete "
            #               f"non-existing level at price: ins={ins_id} "
            #               f"price={price}")
            #     return
            lvls = side[i_price]
            if not lvls.pop(lvl_name, None):
                L.warning(self._tag + "[update_mbp] tried to delete "
                          f"non-existing level at price: ins={ins_id} "
                          f"price={price} lvl_name={lvl_name}")
                return
            if not lvls:
                del side[i_price]
            entry["MDUpdateAction"] = fix.MDUpdateAction.Delete
            if i_price in side_tr:
                entry["MDPriceLevel"] = side_tr.index(i_price) + 1
                if i_price not in side:
                    del side_tr[i_price]
                L.debug("deleted {}".format(entry["MDPriceLevel"]))
                add_entry = True
            # lvls = side_tr.get(i_price)
            # if lvls:
            #     if lvls.pop(lvl_name, None):
            #         L.debug(f"popped {lvl_name}")
            #         add_entry = True
            #     L.debug(pformat(lvls))
            #     if not lvls:
            #         L.debug("deleting lvl!")
            #         del side_tr[i_price]
        elif ua == fix.MDUpdateAction.Overlay:
            #entry["MDUpdateAction"] = fix.MDUpdateAction.Change
            if i_price not in side:
                #entry["MDUpdateAction"] = fix.MDUpdateAction.New
                side[i_price] = {}
            #elif lvl_name not in side[i_price]:
            #    entry["MDUpdateAction"] = fix.MDUpdateAction.New
            pos = side.index(i_price) + 1
            side[i_price][lvl_name] = entry

            if pos <= mbp_depth:
                if i_price not in side_tr:
                    side_tr[i_price] = side[i_price]
                    entry["MDUpdateAction"] = fix.MDUpdateAction.New
                    L.debug("overlayed, new")
                else:
                    entry["MDUpdateAction"] = fix.MDUpdateAction.Change
                    L.debug("overlayed, change")
                entry["MDPriceLevel"] = side_tr.index(i_price) + 1
                L.debug("MDPriceLevel = {}".format(entry["MDPriceLevel"]))
                add_entry = True

            # if pos <= mbp_depth:
                # if i_price not in side_tr:
                #     side_tr[i_price] = lvls = side[i_price]

            # if i_price in side_tr:
            #     entry["MDPriceLevel"] = side_tr.index(i_price) + 1
            #     add_entry = True

            # # if pos <= mbp_depth:
            # if i_price not in side_tr:
            #     side_tr[i_price] = side[i_price]
            # #     side_tr[i_price] = {}
            # # side_tr[i_price][lvl_name] = entry
            # add_entry = True
        elif ua == fix.MDUpdateAction.Change:
            try:
                lvls = side[i_price]
            except KeyError:
                L.warning(self._tag + "[update_mbp] tried to change "
                          "non-existing level at price: ins_id={ins_id} "
                          "price={price}")
                return
            # pos = side.index(i_price) + 1
            #if pos <= mbp_depth:
            lvls[lvl_name] = entry
            if i_price in side_tr:
                entry["MDPriceLevel"] = side_tr.index(i_price) + 1
                L.debug("changed: MDPriceLevel: {}"
                        .format(entry["MDPriceLevel"]))
                add_entry = True
            # lvls = side_tr.get(i_price)
            # if lvls:
            #     add_entry = True
            #     lvls[lvl_name] = entry
        elif ua == fix.MDUpdateAction.New:
            try:
                lvls = side[i_price]
            except KeyError:
                side[i_price] = lvls = {}
            else:
                if lvl_name in lvls:
                    L.warning(self._tag + "[update_mbp] MDUpdateAction.New "
                              f"overwriting existing level: ins_id={ins_id}, "
                              f"price={price}")
                    entry["MDUpdateAction"] = fix.MDUpdateAction.Change
            pos = side.index(i_price) + 1
            # if pos <= mbp_depth:
            lvls[lvl_name] = entry
            if pos <= mbp_depth:
                side_tr[i_price] = lvls
                entry["MDPriceLevel"] = side_tr.index(i_price) + 1
                L.debug("new {}".format(entry["MDPriceLevel"]))
                add_entry = True
            # lvls = side_tr.get(i_price)
            # if not lvls:
            #     side_tr[i_price] = lvls = {}
            # lvls[lvl_name] = entry
        else:
            raise RuntimeError(f"unrecognized MDUpdateAction: {ua}")
        if add_entry:
            sel_entries.append(entry)
        if side_tr.get(i_price) == {}:
            import ipdb; ipdb.set_trace()
            pass
        L.debug(len(side_tr))
        if g.debug_mode:
            if side_tr.get(i_price):
                if id(side_tr[i_price]) != id(side[i_price]):
                    raise RuntimeError("reference must be shared")
            # sel_len = min(len(side_tr), mbp_depth)
            # if side.iloc[:sel_len] != side_tr.iloc[:sel_len]:
            #     print(side.iloc[:sel_len])
            #     print(side_tr.iloc[:sel_len])
            #     import ipdb; ipdb.set_trace()
            #     raise RuntimeError("truncated book and full book do not match")

    
    def _update_generic_md(self, ins_id, market_key, et, entry):
        ins_data = self._ins_data[ins_id]
        gen_md = ins_data["markets"][market_key]["generic_md"]
        ua = entry["MDUpdateAction"]
        L.debug(self._tag + "[gen_md] ins_id={}, len={}, et={}, ua={}".format(
                ins_id, len(gen_md), et, ua))
        if ua == fix.MDUpdateAction.Delete:
            if not gen_md.pop(et, None):
                L.warning("[update_generic_md] tried to delete non-existing "
                          "entry: ins_id={ins_id} et={et}")
                return []
        entry["MDUpdateAction"] = fix.MDUpdateAction.Overlay
        gen_md[et] = entry
        return [entry]


    def _truncate_mbp_handle_book_side(
            self, side, side_tr, mbp_depth, updates):
        tr_len = len(side_tr)
        L.debug(self._tag + f"tr_len={tr_len}")
        if tr_len < mbp_depth:
            # check for potential redisplay of data
            prices = set(side.iloc[:mbp_depth])
            prices_tr = set(side_tr.keys())
            for i_price in prices.difference(prices_tr):
                L.debug(self._tag + f"redisplaying: i_price={i_price}")
                side_tr[i_price] = lvls = side[i_price]
                for entry in lvls.values():
                    entry = deepcopy(entry)
                    entry["MDUpdateAction"] = fix.MDUpdateAction.New
                    entry["MDPriceLevel"] = side_tr.index(i_price) + 1
                    updates.append(entry)
        tr_len = len(side_tr)
        if tr_len > mbp_depth:
            # check for truncation
            i_prices_to_del = side_tr.iloc[mbp_depth:]
            for i_price in i_prices_to_del:
                L.debug(self._tag + f"truncating: i_price={i_price}")
                lvls = side_tr.pop(i_price)
                for entry in lvls.values():
                    entry = deepcopy(entry)
                    entry["MDUpdateAction"] = fix.MDUpdateAction.Delete
                    entry["MDPriceLevel"] = mbp_depth + 1
                    updates.append(entry)
        if g.debug_mode:
            tr_len = len(side_tr)
            if side.iloc[:tr_len] != side_tr.iloc[:tr_len]:
                import ipdb; ipdb.set_trace()
                raise RuntimeError("truncated book and full book do not match")
            target_len = min(len(side), mbp_depth)
            assert len(side_tr) == target_len, (len(side), len(side_tr))


    def _truncate_mbp(self, market_key, mbp_entries):
        for (ins_id, lot_type), entries in mbp_entries.items():
            ins_data = self._ins_data[ins_id]
            mbp_depth = ins_data["mbp_depth"]
            max_mbp_levels = ins_data["max_mbp_levels"]
            if mbp_depth == max_mbp_levels and not self._us_implicit_delete:
                # truncation done automatically by upstream
                continue
            book = ins_data["markets"][market_key]["lot_type"][lot_type]["mbp"]
            self._truncate_mbp_handle_book_side(book["bids"],
                                                book["bids_tr"],
                                                mbp_depth,
                                                entries)
            self._truncate_mbp_handle_book_side(book["asks"],
                                                book["asks_tr"],
                                                mbp_depth,
                                                entries)


    def _handle_md_snapshot(self, msg):
        body = msg["Body"]
        market_key = (body.get("ZMMarketID"), body.get("ZMTradingSessionID"))
        ins_id = body["ZMInstrumentID"]
        if ins_id not in self._ins_data:
            return None
        self._ins_data[ins_id]["markets"].pop(market_key, "")
        for x in body["NoMDEntries"]:
            x["MDUpdateAction"] = fix.MDUpdateAction.New
        res = self._handle_md_inc_refresh(msg)
        if res is None:
            return None
        for entry in res["Body"]["NoMDEntries"]:
            del entry["MDUpdateAction"]
        return res


    def _handle_md_inc_refresh(self, msg):
        body = msg["Body"]
        market_key = (body.get("ZMMarketID"), body.get("ZMTradingSessionID"))
        ins_id = body.get("ZMInstrumentID")
        # key: (ins_id, lot_type)
        mbp_entries = defaultdict(list)
        entries_by_insid = defaultdict(list)
        for entry in body["NoMDEntries"]:
            lot_type = entry.get("LotType")
            ins_id = entry.pop("ZMInstrumentID", ins_id)
            ins_data = self._ins_data.get(ins_id)
            if ins_data is None:
                continue
            mbp_depth = ins_data["mbp_depth"]
            et = entry["MDEntryType"]
            if not self._all_ets and et not in self._ets:
                continue
            if et in (fix.MDEntryType.Bid,
                      fix.MDEntryType.Offer,
                      fix.MDEntryType.EmptyBook):
                book_type = entry.get("MDBookType")
                if "OrderID" in entry \
                        or book_type == fix.MDBookType.OrderDepth:
                    entries_by_insid[ins_id] += self._update_mbo(
                            ins_id,
                            market_key,
                            lot_type,
                            et,
                            entry,
                            mbp_entries)
                else:
                    if entry.get("MDPriceLevel") == 1:
                        entries_by_insid[ins_id] += self._update_generic_md(
                                ins_id, market_key, et, entry)
                    elif self._generate_bbo or mbp_depth > 1:
                        self._update_mbp(
                                ins_id, market_key, lot_type,
                                et, entry, mbp_entries)
            elif et in (fix.MDEntryType.SimulatedBuyPrice,
                        fix.MDEntryType.SimulatedSellPrice):
                if entry.get("MDPriceLevel") == 1:
                    entries_by_insid[ins_id] += self._update_generic_md(
                            ins_id, market_key, et, entry)
                elif self._generate_bbo or mbp_depth > 1:
                    self._update_mbp(
                            ins_id, market_key, lot_type,
                            et, entry, mbp_entries)
            else:
                if lot_type:
                    L.warning(self._tag + "unexpected lot_type when updating"
                              "generic_md:\{}".format(pformat(entry)))
                entries_by_insid[ins_id] += self._update_generic_md(
                        ins_id, market_key, et, entry)
        self._truncate_mbp(market_key, mbp_entries)
        group = []
        # for ins_id, entries in entries_by_insid.items():
        #     entries[0]["ZMInstrumentID"] = ins_id
        #     group += entries

        added_insids = set()
        for (ins_id, lot_type), entries in mbp_entries.items():
            ins_data = self._ins_data[ins_id]
            mbp_depth = ins_data["mbp_depth"]
            to_add = []
            if ins_id in entries_by_insid:
                to_add += entries_by_insid[ins_id]
            if entries:
                if mbp_depth > 1:
                    to_add += entries
                if self._generate_bbo:
                    ets_to_emit = {x["MDEntryType"] for x in entries
                                   if x.get("MDPriceLevel") == 1}
                    books = ins_data["markets"][market_key]["lot_type"][lot_type]["mbp"]
                    for et in ets_to_emit:
                        if et == fix.MDEntryType.Bid:
                            b = books["bids_tr"]
                            lvl = b[b.iloc[0]]["normal"]
                        elif et == fix.MDEntryType.Offer:
                            b = books["asks_tr"]
                            lvl = b[b.iloc[0]]["normal"]
                        elif et == fix.MDEntryType.SimulatedSellPrice:
                            b = books["bids_tr"]
                            lvl = b[b.iloc[0]]["implied"]
                        elif et == fix.MDEntryType.SimulatedBuyPrice:
                            b = books["asks_tr"]
                            lvl = b[b.iloc[0]]["implied"]
                        else:
                            L.critical(f"unexpected et: {et}")
                            sys.exit(1)
                        lvl = deepcopy(lvl)
                        lvl["MDUpdateAction"] = fix.MDUpdateAction.Overlay
                        lvl["MDPriceLevel"] = 1
                        # need to add to generic md here?
                        to_add.append(lvl)
                group += to_add
                added_insids.add(ins_id)
            if to_add:
                to_add[0]["ZMInstrumentID"] = ins_id
        for ins_id, entries in entries_by_insid.items():
            if ins_id not in added_insids:
                entries[0]["ZMInstrumentID"] = ins_id
                group += entries
        body["NoMDEntries"] = group

        if g.debug_mode and self._aggbook and self._mbp_depth > 1:
            ins_id = None
            market_key = (msg["Body"].get("ZMMarketID"),
                          msg["Body"].get("ZMTradingSessionID"))
            instruments = set()
            L.debug("modifying debug books ({} entries)...".format(len(group)))
            for entry in group:
                ins_id = entry.get("ZMInstrumentID", ins_id)
                lot_type = entry.get("LotType")
                et = entry["MDEntryType"]
                ins_data = self._ins_data[ins_id]
                books = ins_data["markets"][market_key]["lot_type"][lot_type]["mbp"]
                if et == fix.MDEntryType.EmptyBook:
                    books["debug_0"].clear()
                    books["debug_1"].clear()
                    books["debug_E"].clear()
                    books["debug_F"].clear()
                    continue
                book = books.get("debug_" + et)
                if book is None:
                    continue
                idx = entry["MDPriceLevel"] - 1
                ua = entry["MDUpdateAction"]
                L.debug(f"{ins_id} {et} {idx} {ua}")
                instruments.add((ins_id, market_key, lot_type))
                r2i = ins_data["r2i"]
                i_price = r2i(entry["MDEntryPx"])
                try:
                    if ua == fix.MDUpdateAction.New:
                        L.debug(f"insert {et} {idx} {i_price}")
                        book.insert(idx, i_price)
                    elif ua == fix.MDUpdateAction.Change:
                        # pass
                        L.debug(f"change {et} {idx} {i_price}")
                        if book[idx] != i_price:
                            import ipdb; ipdb.set_trace()
                            raise RuntimeError(
                                    "unexpected price when changing "
                                    "{} vs {}".format(book[idx], i_price))
                        # book[idx] = i_price
                    elif ua == fix.MDUpdateAction.Delete:
                        del_price = book[idx]
                        L.debug(f"delete {et} {idx} {del_price} "
                                f"(should be {i_price})")
                        del book[idx]
                        if del_price != i_price:
                            import ipdb; ipdb.set_trace()
                            raise RuntimeError(
                                    "unexpected price when deleting on "
                                    f"debugging book: {del_price} "
                                    f"(expected {i_price})")
                except:
                    L.exception("oops")
                    import ipdb; ipdb.set_trace()
                    raise
            for ins_id, market_key, lot_type in instruments:
                ins_data = self._ins_data[ins_id]
                books = ins_data["markets"][market_key]["lot_type"][lot_type]["mbp"]
                x = set(books["debug_0"] + books["debug_E"])
                if x != set(books["bids_tr"].keys()):
                    import ipdb; ipdb.set_trace()
                    RuntimeError("test book (bids) entry prices differ "
                                 "from interal truncated books")
                x = set(books["debug_1"] + books["debug_F"])
                if x != set(books["asks_tr"].keys()):
                    import ipdb; ipdb.set_trace()
                    RuntimeError("test book (asks) entry prices differ "
                                 "from interal truncated books")
                if any(np.diff(books["debug_0"]) > 0):
                    import ipdb; ipdb.set_trace()
                    RuntimeError("order scrambled on debug_0")
                if any(np.diff(books["debug_E"]) > 0):
                    import ipdb; ipdb.set_trace()
                    RuntimeError("order scrambled on debug_E")
                if any(np.diff(books["debug_1"]) < 0):
                    import ipdb; ipdb.set_trace()
                    RuntimeError("order scrambled on debug_1")
                if any(np.diff(books["debug_F"]) < 0):
                    import ipdb; ipdb.set_trace()
                    RuntimeError("order scrambled on debug_F")

        if group:
            return msg
        return None


    async def process_msg(self, msg):
        print("handler", self.req_id)
        header = msg["Header"]
        body = msg["Body"]
        endpoint = header.get("ZMEndpoint")
        if endpoint != self._endpoint:
            return
        msg_type = header["MsgType"]
        if msg_type not in ["X", "W"]:
            return
        if msg_type == "W":
            req_id = body.get("ZMReqID")
            if req_id is not None and req_id not in self._snap_req_ids:
                return
        if self.markets:
            market_id = body.get("ZMMarketID")
            if market_id and market_id not in self.markets:
                return
        if self.tsessions:
            ts_id = body.get("ZMTradingSessionID")
            if ts_id and ts_id not in self.tsessions:
                return
        L.debug(self._tag + "processing message ({} entries)..."
                .format(len(msg["Body"]["NoMDEntries"])))
        if msg_type == "X":
            msg_out = self._handle_md_inc_refresh(msg)
        elif msg_type == "W":
            msg_out = self._handle_md_snapshot(msg)
        else:
            raise RuntimeError(f"unexpected MsgType: {msg_type}")
        if msg_out:
            msg["ZMReqID"] = self.req_id
            seq_no = await g.pub.publish_msg(msg_out)
            L.debug(self._tag + f"publishing MsgSeqNum={seq_no} ...")


###############################################################################


class Subscription(dict):


    def __init__(self, d=None):
        super().__init__()
        if d is not None:
            self.merge(d, inplace=True)
    

    def merge(self, x, inplace=False):
        # self can be empty dict, whereas basic requirements are imposed on x
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
            res["MarketDepth"] = max(self.get("MarketDepth", 1), x["MarketDepth"])
        if "ZMMBOBook" in self or "ZMMBOBook" in x:
            res["ZMMBOBook"] = self.get("ZMMBOBook", False) | x["ZMMBOBook"]
        if "ZMMBPBook" in self or "ZMMBPBook" in x:
            res["ZMMBPBook"] = self.get("ZMMBPBook", False) | x["ZMMBPBook"]
        if "AggregatedBook" in self or "AggregatedBook" in x:
            res["AggregatedBook"] = min(self.get("AggregatedBook", True),
                                        x.get("AggregatedBook", True))
        if "*" in self.get("NoMDEntryTypes", "") or "*" in x["NoMDEntryTypes"]:
            res["NoMDEntryTypes"] = "*"
        else:
            ets = set(self.get("NoMDEntryTypes", "") + x["NoMDEntryTypes"])
            res["NoMDEntryTypes"] = "".join(sorted(ets))
        if "NoTradingSessions" in x and "NoTradingSessions" in self:
            group = []
            group += self["NoTradingSessions"]
            for ts_b in x["NoTradingSessions"]:
                for ts_a in self["NoTradingSessions"]:
                    if ts_b == ts_a:
                        break
                else:
                    group.append(ts_b)
            res["NoTradingSessions"] = group
        return res


    def contains(self, x):
        if self["MarketDepth"] > 0 and x["MarketDepth"] > self["MarketDepth"]:
            return False
        if "ZMMBOBook" in self or "ZMMBOBook" in x:
            if not self["ZMMBOBook"] and x["ZMMBOBook"]:
                return False
            if not self["ZMMBPBook"] and x["ZMMBPBook"]:
                return False
        else:
            if self["AggregatedBook"] and not x["AggregatedBook"]:
                return False
        if "*" not in self["NoMDEntryTypes"]:
            if set(x["NoMDEntryTypes"]).difference(self["NoMDEntryTypes"]):
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


    def __init__(self, endpoint, snap_sem_depth=500):
        # key in value dict: ZMReqID
        def create_empty_sub_dict():
            return SortedDict(lambda x: x[0])
        self._ins_md_subs = defaultdict(create_empty_sub_dict)
        self._market_def_subs = defaultdict(create_empty_sub_dict)
        self._ts_status_subs = defaultdict(create_empty_sub_dict)
        self._ins_def_subs = defaultdict(create_empty_sub_dict)
        self._ins_status_subs = defaultdict(create_empty_sub_dict)
        self._sec_list_subs = {}
        self._endpoint = endpoint
        self.req_id_now = 0  # for external use
        self.ins_md_locks = defaultdict(asyncio.Lock)
        self.ins_status_locks = defaultdict(asyncio.Lock)
        self.ins_def_locks = defaultdict(asyncio.Lock)
        self.snapshot_semaphore = asyncio.Semaphore(snap_sem_depth)
        self._active = True
        self._us_zmreqid_prefix = None
        self._next_zmreqid = 0
        create_task(self._pinger())


    async def initialize(self):
        L.debug(f"initializing SubscriptionManager for {self._endpoint} ...")
        r = await g.ctl.send_recv_command(
                fix.MsgType.ZMGetValidReqID,
                endpoint=self._endpoint,
                check_error=True)
        # print("VITTU", r)
        self._us_zmreqid_prefix = r["Body"]["ZMReqID"]
        L.debug(f"upstream ZMReqID prefix: {repr(self._us_zmreqid_prefix)}")


    def get_valid_us_zmreqid(self):
        res = f"{self._us_zmreqid_prefix}{self._next_zmreqid}"
        self._next_zmreqid += 1
        return res


    def __del__(self):
        self._active = False


    async def _pinger(self):
        endpoint = self._endpoint
        L.debug(f"SubscriptionManager._pinger() for {endpoint} started")
        feats = await g.ctl.get_con_feats(endpoint)
        hb_interval = feats["HeartbeatInterval"]
        while self._active:
            await g.ctl.send_recv_command(
                    fix.MsgType.TestRequest, endpoint=endpoint)
            await asyncio.sleep(hb_interval)
        L.debug(f"SubscriptionManager._pinger() for {endpoint} exited")

    
    # @staticmethod
    # def _merge_md_subs(a, b):
    #     # a can be empty dict, whereas basic requirements are imposed on b
    #     res = {}
    #     if a.get("MarketDepth", 1) == 0 or b["MarketDepth"] == 0:
    #         res["MarketDepth"] = 0
    #     else:
    #         res["MarketDepth"] = max(a.get("MarketDepth", 1), b["MarketDepth"])
    #     if "ZMMBOBook" in a or "ZMMBOBook" in b:
    #         res["ZMMBOBook"] = a.get("ZMMBOBook", False) | b["ZMMBOBook"]
    #     if "ZMMBPBook" in a or "ZMMBPBook" in b:
    #         res["ZMMBPBook"] = a.get("ZMMBPBook", False) | b["ZMMBPBook"]
    #     if "AggregatedBook" in a or "AggregatedBook" in b:
    #         res["AggregatedBook"] = min(a.get("AggregatedBook", True),
    #                                     b.get("AggregatedBook", True))
    #     if "*" in a.get("NoMDEntryTypes", "") or "*" in b["NoMDEntryTypes"]:
    #         res["NoMDEntryTypes"] = "*"
    #     else:
    #         ets = set(a.get("NoMDEntryTypes", "") + b["NoMDEntryTypes"])
    #         res["NoMDEntryTypes"] = "".join(sorted(ets))
    #     if "NoTradingSessions" in b and "NoTradingSessions" in a:
    #         group = []
    #         group += a["NoTradingSessions"]
    #         for ts_b in b["NoTradingSessions"]:
    #             for ts_a in a["NoTradingSessions"]:
    #                 if ts_b == ts_a:
    #                     break
    #             else:
    #                 group.append(ts_b)
    #         res["NoTradingSessions"] = group
    #     return res


    # @staticmethod
    # def _md_sub_contains(a, b):
    #     if a["MarketDepth"] > 0 and b["MarketDepth"] > a["MarketDepth"]:
    #         return False
    #     if "ZMMBOBook" in a or "ZMMBOBook" in b:
    #         if not a["ZMMBOBook"] and b["ZMMBOBook"]:
    #             return False
    #         if not a["ZMMBPBook"] and b["ZMMBPBook"]:
    #             return False
    #     else:
    #         if a["AggregatedBook"] and not b["AggregatedBook"]:
    #             return False
    #     if "*" not in a["NoMDEntryTypes"]:
    #         if b["NoMDEntryTypes"].difference(a["NoMDEntryTypes"]):
    #             return False
    #     if "NoTradingSessions" in a:
    #         if "NoTradingSessions" not in b:
    #             return False
    #         for ts_b in b["NoTradingSessions"]:
    #             for ts_a in a["NoTradingSessions"]:
    #                 if ts_b == ts_a:
    #                     break
    #             else:
    #                 return False
    #     return True

    @staticmethod
    def _merge_subs(subs, sub_cmp=None):
        merged = None
        ts_contained = None
        for (ts, _), d in subs.items():
            if merged is None:
                merged = d
            else:
                merged = merged.merge(d)
            if sub_cmp is not None \
                    and ts_contained is None \
                    and merged.contains(sub_cmp):
                ts_contained = ts
        return merged, ts_contained


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


    # async def _update_upstream_md_sub(
    #         self, ins_id, market_id, market_seg_id, new_sub):
    #     # ins_id, market_id, market_seg_id = sub_key
    #     # if new_sub is None:
    #     #     srt = fix.SubscriptionRequestType.Unsubscribe
    #     # else:
    #     #     srt = fix.SubscriptionRequestType.SnapshotAndUpdates
    #     # body = {} if new_sub is None else deepcopy(new_sub)
    #     body = self._update_upstream_sub_base(new_sub)
    #     body["ZMInstrumentID"] = ins_id
    #     if market_id:
    #         body["MarketID"] = market_id
    #     if market_seg_id:
    #         body["MarketSegmentID"] = market_seg_id
    #     body["SubscriptionRequestType"] = srt
    #     res = await g.ctl.send_recv_command(
    #             fix.MsgType.MarketDataRequest,
    #             body=body,
    #             endpoint=self._endpoint,
    #             check_error=True)
    #     return res


    @staticmethod
    def _update_upstream_sub_base(new_sub):
        if new_sub is None:
            srt = fix.SubscriptionRequestType.Unsubscribe
        else:
            srt = fix.SubscriptionRequestType.SnapshotAndUpdates
        body = {} if new_sub is None else deepcopy(new_sub)
        body["SubscriptionRequestType"] = srt
        return body




    # async def _update_upstream_simple_sub(self, ins_id, subscribe, msg_type):
    #     if subscribe:
    #         srt = fix.SubscriptionRequestType.SnapshotAndUpdates
    #     else:
    #         srt = fix.SubscriptionRequestType.Unsubscribe
    #     body = {}
    #     body["SubscriptionRequestType"] = srt
    #     body["ZMInstrumentID"] = ins_id
    #     res = await g.ctl.send_recv_command(
    #             msg_type, body=body, endpoint=self._endpoint, check_error=True)
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


    # async def subscribe_ins_md(self, ins_id, body):
    #     body = deepcopy(body)
    #     body["SubscriptionRequestType"] = \
    #             fix.SubscriptionRequestType.SnapshotAndUpdates
    #     return await self._update_ins_md_sub(ins_id, body)


    async def _snap_poller_base(self,
                                sub_key,
                                subs,
                                req_msg_type,
                                sub_mutator_fn=None,
                                res_mutator_fn=None,
                                interval=5):
        L.debug("snap poller running (MsgType: {}, sub_key: {}) ..."
                .format(req_msg_type, sub_key))
        us_zmreq_id_support = type(subs) == dict
        while True:
            if us_zmreq_id_support:
                body = subs.get(sub_key)
                if body is None:
                    break
                body = deepcopy(body)
            else:
                body = self._merge_subs(subs)
                if body is None:
                    break
            if sub_mutator_fn:
                sub_mutator_fn(body)
            body["SubscriptionRequestType"] = \
                    fix.SubscriptionRequestType.Snapshot
            body["ZMSendToPub"] = False
            try:
                res = await g.ctl.send_recv_command(
                        req_msg_type,
                        body=body,
                        endpoint=self._endpoint,
                        check_error=True)
            except RejectException:
                L.error("error on snap poller ({}):".format(sub_key))
            else:
                res["Header"]["ZMEndpoint"] = self._endpoint
                if us_zmreq_id_support:
                    res["Body"]["ZMReqID"] = sub_key
                res_mutator_fn(res)
                await g.sock_internal_pub.send_string(" " + json.dumps(res))
            # TODO: make sleep time customizable
            await asyncio.sleep(interval)
        L.debug("snap poller exited ({})".format(sub_key))

    
    async def _update_sub(self,
                          sub,
                          sub_key,
                          req_id,
                          srt,
                          subs,
                          req_msg_type,
                          sub_mutator_fn,
                          oob_snap_cap,
                          subscription_cap,
                          poller_res_mutator_fn=None):

        assert sub is not None
        ts_now = get_timestamp()
        ts_contained = None
        caps = await g.ctl.get_caps(self._endpoint)
        
        if subscription_cap not in caps:
            assert oob_snap_cap in caps

        prev_sub_entry = None
        for k in subs.keys():
            if k[1] == req_id:
                prev_sub_entry = (k, subs[k])
                break

        if srt == fix.SubscriptionRequestType.Unsubscribe:
            if prev_sub_entry is None:
                raise ValueError("no subscription found")
            old_merged_sub, _ = self._merge_subs(subs)
            del subs[prev_sub_entry[0]]
            new_merged_sub, _ = self._merge_subs(subs)
            if subscription_cap in caps and new_merged_sub != old_merged_sub:
                try:
                    body = self._update_upstream_sub_base(new_merged_sub)
                    sub_mutator_fn(body)
                    await g.ctl.send_recv_command(
                            req_msg_type,
                            body=body,
                            endpoint=self._endpoint,
                            check_error=True)
                except RejectException:
                    subs[prev_sub_entry[0]] = prev_sub_entry[1]
                    raise

        else:
            assert prev_sub_entry is None, req_id
            old_merged_sub, ts_contained = self._merge_subs(subs, sub)
            subs[(ts_now, req_id)] = sub
            new_merged_sub, _ = self._merge_subs(subs)
            if subscription_cap in caps:
                if new_merged_sub != old_merged_sub:
                    try:
                        body = self._update_upstream_sub_base(new_merged_sub)
                        sub_mutator_fn(body)
                        res = await g.ctl.send_recv_command(
                                req_msg_type,
                                body=body,
                                endpoint=self._endpoint,
                                check_error=True)
                    except RejectException:
                        del subs[(ts_now, req_id)]
                        raise
                    else:
                        ts_contained = res["Body"].get("_TimeSubscribed")
            else:
                if old_merged_sub is None and new_merged_sub is not None:
                    create_task(self._snap_poller_base(
                        sub_key=sub_key,
                        subs=subs,
                        req_msg_type=req_msg_type,
                        sub_mutator_fn=sub_mutator_fn,
                        res_mutator_fn=poller_res_mutator_fn))

        return ts_contained


    async def update_ins_md_sub(self, body, req_id, ins_id, market_id, ts_id):
        srt = body["SubscriptionRequestType"]
        if srt == fix.SubscriptionRequestType.SnapshotAndUpdates:
            sub = MDSubscription(body)
        else:
            sub = body
        sub_key = (ins_id, market_id, ts_id)
        subs = self._ins_md_subs[sub_key]
        caps = await g.ctl.get_caps(self._endpoint)
        req_msg_type = fix.MsgType.MarketDataRequest
        def sub_mutator_fn(sub):
            sub["ZMInstrumentID"] = ins_id
            if market_id:
                sub["ZMMarketID"] = market_id
            if ts_id:
                sub["ZMTradingSessionID"] = ts_id
        return await self._update_sub(
                sub=sub,
                sub_key=sub_key,
                req_id=req_id,
                srt=srt,
                subs=subs,
                req_msg_type=fix.MsgType.MarketDataRequest,
                sub_mutator_fn=sub_mutator_fn,
                oob_snap_cap=fix.ZMCap.MDOOBSnapshot,
                subscription_cap=fix.ZMCap.MDSubscribe)


    # async def unsubscribe_ins_md_sub(
    #         self, req_id, ins_id, market_id, market_seg_id):
    #     body = {}
    #     body["SubscriptionRequestType"] = \
    #             fix.SubscriptionRequestType.Unsubscribe
    #     body["ZMInstrumentID"] = ins_id
    #     await self.update_ins_md_sub(
    #             ins_id, market_id, market_seg_id, body, req_id)


    async def update_ins_status_sub(self, body, req_id, ins_id):
        srt = body["SubscriptionRequestType"]
        sub = ParameterlessSubscription(body)
        subs = self._ins_status_subs[ins_id]
        def sub_mutator_fn(sub):
            sub["ZMInstrumentID"] = ins_id
        return await self._update_sub(
                sub=sub,
                sub_key=ins_id,
                reqid=req_id,
                srt=srt,
                subs=subs,
                req_msg_type=fix.MsgType.SecurityStatusRequest,
                sub_mutator_fn=sub_mutator_fn,
                oob_snap_cap=fix.ZMCap.SecurityStatusOOBSnapshot,
                subscription_cap=fix.ZMCap.SecurityStatusSubscribe)
        

    # async def unsubscribe_ins_status_sub(self, req_id, ins_id):
    #     body = {}
    #     body["SubscriptionRequestType"] = \
    #             fix.SubscriptionRequestType.Unsubscribe
    #     # body["ZMInstrumentID"] = ins_id
    #     await self.update_ins_status_sub(ins_id, body, req_id)


    async def update_ins_def_sub(self, body, req_id, ins_id):
        srt = body["SubscriptionRequestType"]
        sub = ParameterlessSubscription(body)
        subs = self._ins_def_subs[ins_id]
        def sub_mutator_fn(sub):
            sub["ZMInstrumentID"] = ins_id
        return await self._update_sub(
                sub=sub,
                sub_key=ins_id,
                req_id=req_id,
                srt=srt,
                subs=subs,
                req_msg_type=fix.MsgType.SecurityDefinitionRequest,
                sub_mutator_fn=sub_mutator_fn,
                oob_snap_cap=fix.ZMCap.SecurityDefinitionOOBSnapshot,
                subscription_cap=fix.ZMCap.SecurityDefinitionSubscribe)


    async def _update_sub_reqid(self,
                                body,
                                req_id,
                                subs,
                                subscription_cap,
                                oob_snap_cap,
                                req_msg_type,
                                poller_interval,
                                poller_res_mutator_fn=None):

        srt = body["SubscriptionRequestType"]
        caps = await g.ctl.get_caps(self._endpoint)
        
        if subscription_cap not in caps:
            assert oob_snap_cap in caps

        if srt == fix.SubscriptionRequestType.Unsubscribe:
            if req_id not in subs:
                raise ValueError("subscription not found")
            del subs[req_id]
            if subscription_cap in caps:
                try:
                    await g.ctl.send_recv_command(
                            req_msg_type,
                            body=body,
                            endpoint=self._endpoint,
                            check_error=True)
                except RejectException:
                    subs[req_id] = body
                    raise

        else:

            if req_id in subs:
                raise ValueError("subscription with duplicate ZMReqID found")
            subs[req_id] = body
            if subscription_cap in caps:
                try:
                    await g.ctl.send_recv_command(
                            req_msg_type,
                            body=body,
                            endpoint=self._endpoint,
                            check_error=True)
                except RejectException:
                    del subs[req_id]
                    raise
            else:
                create_task(self._snap_poller_base(
                    sub_key=req_id,
                    subs=subs,
                    req_msg_type=req_msg_type,
                    res_mutator_fn=poller_res_mutator_fn,
                    interval=poller_interval))

        return 0


    async def update_sec_list_sub(self, body, req_id, *params):
        def poller_res_mutator_fn(res):
            res["Header"]["MsgType"] = fix.MsgType.SecurityList
        return await self._update_sub_reqid(
                body=deepcopy(body),
                req_id=req_id,
                subs=self._sec_list_subs,
                subscription_cap=fix.ZMCap.SecurityListSubscribe,
                oob_snap_cap=fix.ZMCap.SecurityListOOBSnapshot,
                req_msg_type=fix.MsgType.SecurityListRequest,
                # TODO: read interval from sessionizer settings
                poller_interval=60,
                poller_res_mutator_fn=poller_res_mutator_fn)


    # async def update_sec_list_sub(self, body, req_id, *params):
    #     # TODO: standardize this, for other upstream ZMReqID based
    #     # subscriptions ...
    #     srt = body["SubscriptionRequestType"]
    #     caps = await g.ctl.get_caps(self._endpoint)
    #     subs = self._sec_list_subs
    #     def poller_res_mutator_fn(res):
    #         res["Header"]["MsgType"] = fix.MsgType.SecurityList
    #     if fix.ZMCap.SecurityListSubscribe not in caps:
    #         assert fix.ZMCap.SecurityListOOBSnapshot in caps
    #     if srt == fix.SubscriptionRequestType.SnapshotAndUpdates:
    #         if req_id in subs:
    #             raise ValueError("subscription with duplicate ZMReqID found")
    #         subs[req_id] = deepcopy(body)
    #         if fix.ZMCap.SecurityListSubscribe in caps:
    #             await g.ctl.send_recv_command(
    #                     fix.MsgType.SecurityListRequest,
    #                     body=body,
    #                     endpoint=self._endpoint,
    #                     check_error=True)
    #         else:
    #             create_task(self._snap_poller_base(
    #                 sub_key=req_id,
    #                 subs=subs,
    #                 req_msg_type=fix.MsgType.SecurityListRequest,
    #                 res_mutator_fn=poller_res_mutator_fn),
    #                 # TODO: read interval from sessionizer settings
    #                 interval=60)
    #     if srt == fix.SubscriptionRequestType.Unsubscribe:
    #         if req_id not in subs:
    #             raise ValueError("subscription not found")
    #         del subs[req_id]
    #         if fix.ZMCap.SecurityListSubscribe in caps:
    #             await g.ctl.send_recv_command(
    #                     fix.MsgType.SecurityListRequest,
    #                     body=body,
    #                     endpoint=self._endpoint,
    #                     check_error=True)
    #     return 0


    # async def unsubscribe_ins_def_sub(self, req_id, ins_id):
    #     body = {}
    #     body["SubscriptionRequestType"] = \
    #             fix.SubscriptionRequestType.Unsubscribe
    #     await self.update_ins_def_sub(self, ins_id, body, req_id)





###############################################################################


async def subscriber_handler(msg):
    for k in list(g.sub_handlers.keys()):
        handler = g.sub_handlers[k]
        try:
            res = await handler.process_msg(deepcopy(msg))
        except:
            L.exception("[handler-{}] exception:".format(handler.req_id))
            import ipdb; ipdb.set_trace()
            del g.sub_handlers[k]
            # TODO: unsubscribe also
        else:
            if res:
                await g.pub.publish_msg(res)


class MySubscriber(Subscriber):


    async def _handle_msg_2(self, topic, msg):
        await subscriber_handler(msg)
        # for k in list(g.sub_handlers.keys()):
        #     handler = g.sub_handlers[k]
        #     try:
        #         res = await handler.process_msg(deepcopy(msg))
        #     except:
        #         L.exception("[handler-{}] exception:".format(handler.req_id))
        #         import ipdb; ipdb.set_trace()
        #         del g.sub_handlers[k]
        #         # TODO: unsubscribe also
        #     else:
        #         if res:
        #             await g.pub.publish_msg(res)


async def subscriber_receiver():

    L.debug("subscriber handler running ...")
    sock_internal_sub = g.ctx.socket(zmq.SUB)
    sock_internal_sub.connect(INTERNAL_PUB_ADDR)
    sock_internal_sub.subscribe(b"")
    poller = zmq.asyncio.Poller()
    poller.register(g.sock_sub)
    poller.register(sock_internal_sub)

    while True:
        events = await poller.poll()
        for sock, _ in events:
            if sock == sock_internal_sub:
                msg = json.loads(await sock.recv_string())
                await subscriber_handler(msg)
            if sock == g.sock_sub:
                await g.sub.handle_one()


###############################################################################


class MyController(MiddlewareCTL):


    def __init__(self, sock_dn, dealer):
        super().__init__(sock_dn, dealer)


    @alru_cache(maxsize=None)
    async def get_con_feats(self, endpoint):
        res = await self.send_recv_command(
                fix.MsgType.ZMGetConnectorFeatures, endpoint=endpoint)
        res = res["Body"]["ZMConnectorFeatures"]
        res = json.loads(b64decode(res.encode()).decode())
        L.debug("Connector features for {}:\n{}".format(endpoint, pformat(res)))
        return res


    @alru_cache(maxsize=None)
    async def get_ins_definition(self, endpoint, ins_id):
        msg = {}
        msg["Header"] = header = {}
        header["MsgType"] = fix.MsgType.SecurityDefinitionRequest
        header["ZMEndpoint"] = endpoint
        msg["Body"] = body = {}
        body["ZMInstrumentID"] = ins_id
        body["SubscriptionRequestType"] = fix.SubscriptionRequestType.Snapshot
        res = await g.ctl.SecurityDefinitionRequest(None, None, msg)
        # pprint(res)
        caps = await g.ctl.get_caps(endpoint)
        # connector sanity check
        if fix.ZMCap.MDMBPExplicitDelete not in caps \
                and not res.get("ZMMaxMBPLevels"):
            L.critical("When MDMBPExplicitDelete is not defined connector "
                       "must provide ZMMaxMBPLevels in "
                       "SecurityDefinition")
            sys.exit(1)
        return res
        

        # pass
        # await g.ctl.SecurityDefinitionRequest(
        # body_out = {
        #     "ZMInstrumentID": ins_id,
        #     "SubscriptionRequestType": fix.SubscriptionRequestType.Snapshot,
        # }
        # res = await self.send_recv_command(
        #         fix.MsgType.SecurityDefinitionRequest,
        #         endpoint=endpoint,
        #         body=body_out,
        #         check_error=True)
        # if not fix.ZMCap.MDMBPExplicitDelete \
        #         and not body.get("ZMMaxMBPLevels"):
        #     L.critical("When MDMBPExplicitDelete is not defined connector "
        #                "must provide ZMMaxMBPLevels to "
        #                "SecurityDefinitionRequest")
        #     sys.exit(1)
        # return res


    @alru_cache(maxsize=None)
    async def get_caps(self, endpoint):
        res = await self.send_recv_command(
                fix.MsgType.ZMListCapabilities, endpoint=endpoint)
        return res["Body"]["ZMNoCaps"]


    def _process_snapshot(self, body, sub_def):
        body = deepcopy(body)
        # trading_sessions = set([(None, None)])
        # for d in sub_def.get("NoTradingSessions", []):
        #     trading_sessions.add((d["TradingSessionID"],
        #                           d.get("TradingSessionSubID", None)))
        ets = set(sub_def["NoMDEntryTypes"])
        market_depth = sub_def["MarketDepth"]
        market_depth = market_depth if market_depth > 0 else sys.maxsize
        agg_book = sub_def.get("AggregatedBook", True)
        entries = OrderedDict()
        for entry in body["NoMDEntries"]:
            # ts_key = (entry.get("TradingSessionID"),
            #           entry.get("TradingSessionSubID"))
            # if ts_key not in trading_sessions:
            #     continue
            et = entry["MDEntryType"]
            if "*" not in ets and et not in ets:
                continue
            price_level = entry.get("MDPriceLevel")
            if price_level and price_level > market_depth:
                continue
            lot_type = entry.get("LotType")
            order_id = entry.get("OrderID")
            if agg_book and order_id:
                continue
            if price_level and not agg_book:
                continue
            entry_key = (lot_type, et, price_level, order_id)
            if market_depth == 1:
                # bbo entries are emitted first, we prefer those
                if entry_key in entries:
                    continue
            entries[entry_key] = entry
        entries = sorted(entries.values(),
                         key=lambda x: (x["MDEntryType"],
                                        x.get("MDPriceLevel")))
        body["NoMDEntries"] = entries
        return body


    async def _sub_request_base(self,
                                msg,
                                sub_handler_class,
                                res_msg_type,
                                update_sub_afn,
                                subscription_cap,
                                oob_snap_cap,
                                snap_wait_time_feat,
                                snap_wait_time_default,
                                get_snapshot_afn,
                                snapshot_params=None,
                                get_snapshot_semaphore_fn=None,
                                snap_rep_group_name=None,
                                get_sub_locks_fn=None,
                                sub_params_generator=None,
                                sub_handler_args=None):
        
        body = msg["Body"]
        header = msg["Header"]
        endpoint = header["ZMEndpoint"]
        caps = await self.get_caps(endpoint)
        feats = await self.get_con_feats(endpoint)
        submgr = g.submgrs[endpoint]
        if sub_handler_args is None:
            sub_handler_args = ()
        if get_sub_locks_fn:
            sub_locks = get_sub_locks_fn(submgr)
        else:
            # use dummy lock dictionary if none is specified
            sub_locks = defaultdict(asyncio.Lock)
        if get_snapshot_semaphore_fn:
            snapshot_semaphore = get_snapshot_semaphore_fn(submgr)
        else:
            snapshot_semaphore = submgr.snapshot_semaphore
        if not snapshot_params:
            # dummy snapshot_params
            snapshot_params = [((None,), None)]
        if not sub_params_generator:
            # dummy sub params generator
            sub_params_generator = lambda x: [((None,), None)]
        srt = body["SubscriptionRequestType"]
        req_id = body.get("ZMReqID")
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = res_msg_type
        res["Body"] = {}

        if srt != fix.SubscriptionRequestType.Snapshot:
            if req_id is None:
                raise RejectException("required field missing: ZMReqID")
            if type(req_id) != str:
                raise RejectException("type of ZMReqID must be str")

        if srt == fix.SubscriptionRequestType.Unsubscribe:
            if req_id not in g.sub_handlers \
                    or type(g.sub_handlers[req_id]) != sub_handler_class:
                raise RejectException("subscription does not exist")
            sub_handler = g.sub_handlers.pop(req_id)
            for params, lock_key in sub_params_generator(sub_handler):
                lock_key = lock_key if lock_key else params
                async with sub_locks[lock_key]:
                    try:
                        sub_body = {}
                        sub_body["SubscriptionRequestType"] = \
                                fix.SubscriptionRequestType.Unsubscribe
                        await update_sub_afn(submgr, sub_body, req_id, *params)
                    except RejectException:
                        L.error("error on unsubscribing {}:".format(params))
            res["Body"]["Text"] = "unsubscribed"
            return res

        if srt == fix.SubscriptionRequestType.Snapshot:
            # if oob_snap_cap in caps:
            #     res["Body"][snap_rep_group_name] = \
            #             await get_oob_snapshot_afn()
            snap_wait_time = feats.get(snap_wait_time_feat,
                                       snap_wait_time_default)
            async def get_snapshot(lock_key, *params):
                async with snapshot_semaphore:
                    # msg_up = deepcopy(msg)
                    # msg_up["Body"]["SubscriptionRequestType"] = \
                    #         fix.SubscriptionRequestType.SnapshotAndUpdates
                    req_id = submgr.req_id_now
                    # msg_up["Body"]["ZMReqID"] = req_id
                    submgr.req_id_now += 1
                    async with sub_locks[lock_key]:
                        if oob_snap_cap not in caps:
                            assert subscription_cap in caps
                            try:
                                ts_contained = await update_sub_afn(
                                        submgr,
                                        body,
                                        req_id,
                                        *params)
                            except RejectException as e:
                                # text
                                s = f'error subscribing to "{params}": '
                                l = list(e.args)
                                l[0] = s + l[0]
                                e.args = tuple(l)
                                raise e
                            assert ts_contained is not None
                            ts_now = get_timestamp()
                            ts_finished = ts_contained + 1e9 * snap_wait_time
                            sleep_secs = (ts_finished - ts_now) / 1e9
                            if sleep_secs > 0:
                                await asyncio.sleep(sleep_secs)
                        try:
                            return await get_snapshot_afn(submgr, *params)
                        finally:
                            if oob_snap_cap not in caps:
                                sub_body = {}
                                sub_body["SubscriptionRequestType"] = \
                                        fix.SubscriptionRequestType.Unsubscribe
                                await update_sub_afn(
                                        submgr, sub_body, req_id, *params)
            tasks = []
            for params, lock_key in snapshot_params:
                lock_key = lock_key if lock_key else params
                tasks.append(create_task(get_snapshot(lock_key, *params)))
            r = await asyncio.gather(*tasks)
            if snap_rep_group_name:
                r = [x for x in r if x]
                res["Body"][snap_rep_group_name] = r
            else:
                res["Body"] = r[0]
            return res

        if srt == fix.SubscriptionRequestType.SnapshotAndUpdates:
            if req_id in g.sub_handlers:
                raise RejectException(
                        "subscription with duplicate ZMReqID exists")
            # TODO: Check that this is no-op when subscribing to same topic
            #       many times.
            g.sock_sub.subscribe(endpoint.encode() + b"\0")
            g.sub_handlers[req_id] = sub_handler = sub_handler_class(msg)
            try:
                await sub_handler.initialize(*sub_handler_args)
            except:
                del g.sub_handlers[req_id]
                raise
            subscribed = []
            try:
                for params, lock_key in sub_params_generator(sub_handler):
                    lock_key = lock_key if lock_key else params
                    async with sub_locks[lock_key]:
                        await update_sub_afn(
                                submgr, body, req_id, *params)
                    subscribed.append((params, lock_key))
            except RejectException:
                del g.sub_handlers[req_id]
                # if any errors, cancel successful subscriptions
                for params, lock_key in subscribed:
                    async with sub_locks[lock_key]:
                        try:
                            sub_body = {}
                            sub_body["SubscriptionRequestType"] = \
                                    fix.SubscriptionRequestType.Unsubscribe
                            await update_sub_afn(
                                    submgr, sub_body, req_id, *params)
                        except RejectException:
                            L.error("error on unsubscribing {}:"
                                    .format(params))
                raise
            res["Body"]["Text"] = "subscribed"
            return res

        raise RejectException("invalid SubscriptionRequestType")


    async def _get_snapshot_base(self, msg, ident, mutate_sub_fn=None):
        msg = deepcopy(msg)
        body = msg["Body"]
        body.pop("NoRelatedSym", None)
        body["SusbcriptionRequestType"] = \
                fix.SubscriptionRequestType.Snapshot
        body["ZMSendToPub"] = False
        if mutate_sub_fn:
            mutate_sub_fn(body)
        res = await self.send_recv_msg(msg, ident=ident)
        if res["Header"]["MsgType"] == fix.MsgType.ZMReject:
            if res["Body"]["ZMRejectReason"] == fix.ZMRejectReason.NoData:
                return {}
            check_if_error(res)
        return res["Body"]


    async def MarketDataRequest(self, ident, msg_raw, msg):
        msg = deepcopy(msg)
        body = msg["Body"]
        body["NoRelatedSym"] = sorted(set(body["NoRelatedSym"]))
        if "NoMarketSegments" in body:
            body["NoMarketSegments"] = sorted(set(body["NoMarketSegments"]))
        if "NoTradingSessions" in body:
            body["NoTradingSessions"] = sorted(set(body["NoTradingSessions"]))
        sub_def = MDSubscription(body)
        srt = body["SubscriptionRequestType"]
        endpoint = msg["Header"]["ZMEndpoint"]
        caps = await self.get_caps(endpoint)
        if srt != fix.SubscriptionRequestType.Unsubscribe:
            if "ZMMBPBook" in body:
                raise RejectException(
                        "invalid field", 
                        fix.ZMRejectReason.InvalidField,
                        "ZMMBPBook")
            if "ZMMBOBook" in body:
                raise RejectException(
                        "invalid field", 
                        fix.ZMRejectReason.InvalidField,
                        "ZMMBPBook")
            if fix.ZMCap.MDMBPPlusMBO in caps:
                if body.pop("AggregatedBook", True):
                    body["ZMMBPBook"] = True
                    body["ZMMBOBook"] = False
                else:
                    body["ZMMBPBook"] = False
                    body["ZMMBOBook"] = True
        # market_id = body.get("ZMMarketID")
        # ts_id = body.get("ZMTradingSessionID")
        instruments = body["NoRelatedSym"]
        markets = body.get("NoMarketSegments", [None])
        tsessions = body.get("NoTradingSessions", [None])
        itr = itertools.product(instruments, markets, tsessions)
        snapshot_params = [(x, None) for x in itr]
        async def get_snapshot_afn(submgr, *params):
            ins_id, market_id, ts_id = params
            def mutate_sub_fn(sub):
                sub["ZMInstrumentID"] = ins_id
                if market_id:
                    sub["ZMMarketID"] = market_id
                if ts_id:
                    sub["ZMTradingSessionID"] = ts_id
            res = await self._get_snapshot_base(
                    msg,
                    ident,
                    mutate_sub_fn)
            return self._process_snapshot(res, sub_def)
        async def update_sub_afn(submgr, body, req_id, *params):
            return await submgr.update_ins_md_sub(body, req_id, *params)
        return await self._sub_request_base(
                msg=msg,
                sub_handler_class=MDSubHandler,
                res_msg_type=fix.MsgType.ZMMarketDataRequestResponse,
                sub_params_generator=lambda x: snapshot_params,
                get_sub_locks_fn=lambda x: x.ins_md_locks,
                update_sub_afn=update_sub_afn,
                subscription_cap=fix.ZMCap.MDSubscribe,
                oob_snap_cap=fix.ZMCap.MDOOBSnapshot,
                snap_wait_time_feat="MarketDataSnapshotWaitTime",
                snap_wait_time_default=10,
                snap_rep_group_name="ZMNoMDSnapshots",
                get_snapshot_semaphore_fn=lambda x: x.snapshot_semaphore,
                get_snapshot_afn=get_snapshot_afn,
                snapshot_params=snapshot_params)
        

    async def SecurityStatusRequest(self, ident, msg_raw, msg):
        msg = deepcopy(msg)
        body = msg["Body"]
        body["NoRelatedSym"] = sorted(set(body["NoRelatedSym"]))
        # srt = body["SubscriptionRequestType"]
        # snapshot_params = None
        # if srt != fix.SubscriptionRequestType.Unsubscribe:
        snapshot_params = [((x,), None) for x in body["NoRelatedSym"]]
        async def get_snapshot_afn(submgr, ins_id):
            return await self._get_snapshot_base(
                    msg,
                    ident,
                    lambda x: x.__setitem__("ZMInstrumentID", ins_id))
        # def sub_params_generator(sub_handler):

        #     res = itertools.product(sub_handler.instruments,
        #                              sub_handler.markets)
        #     return ((x, None) for x in res)
        async def update_sub_afn(submgr, body, req_id, *params):
            return await submgr.update_ins_status_sub(body, req_id, *params)
        return await self._sub_request_base(
                msg=msg,
                sub_handler_class=SecurityStatusSubHandler,
                res_msg_type=fix.MsgType.ZMSecurityStatusRequestResponse,
                sub_params_generator=lambda x: snapshot_params,
                get_sub_locks_fn=lambda x: x.ins_status_locks,
                update_sub_afn=update_sub_afn,
                subscription_cap=fix.ZMCap.SecurityStatusSubscribe,
                oob_snap_cap=fix.ZMCap.SecurityStatusOOBSnapshot,
                snap_wait_time_feat="SecurityStatusSnapshotWaitTime",
                snap_wait_time_default=10,
                snap_rep_group_name="ZMNoSecStatusSnapshots",
                get_snapshot_afn=get_snapshot_afn,
                snapshot_params=snapshot_params)


    async def SecurityDefinitionRequest(self, ident, msg_raw, msg):
        msg = deepcopy(msg)
        body = msg["Body"]
        snapshot_params = [((body["ZMInstrumentID"],), None)]
        async def get_snapshot_afn(submgr, ins_id):
            return await self._get_snapshot_base(
                    msg,
                    ident,
                    lambda x: x.__setitem__("ZMInstrumentID", ins_id))
        async def update_sub_afn(submgr, body, req_id, *params):
            return await submgr.update_ins_def_sub(body, req_id, *params)
        return await self._sub_request_base(
                msg=msg,
                sub_handler_class=SecurityDefinitionSubHandler,
                res_msg_type=fix.MsgType.ZMSecurityDefinitionRequestResponse,
                sub_params_generator=lambda x: snapshot_params,
                get_sub_locks_fn=lambda x: x.ins_def_locks,
                update_sub_afn=update_sub_afn,
                subscription_cap=fix.ZMCap.SecurityDefinitionSubscribe,
                oob_snap_cap=fix.ZMCap.SecurityDefinitionOOBSnapshot,
                snap_wait_time_feat="SecurityDefinitionSnapshotWaitTime",
                snap_wait_time_default=10,
                get_snapshot_afn=get_snapshot_afn,
                snapshot_params=snapshot_params)

    
    async def SecurityListRequest(self, ident, msg_raw, msg):
        endpoint = msg["Header"]["ZMEndpoint"]
        us_req_id = g.submgrs[endpoint].get_valid_us_zmreqid()
        sub_handler_args = [us_req_id]
        async def get_snapshot_afn(submgr, *params):
            return await self._get_snapshot_base(
                    msg,
                    ident,
                    None)
        async def update_sub_afn(submgr, body, req_id, *params):
            return await submgr.update_sec_list_sub(body, us_req_id, *params)
        return await self._sub_request_base(
                msg=msg,
                sub_handler_class=SecurityListSubHandler,
                sub_handler_args=sub_handler_args,
                res_msg_type=fix.MsgType.ZMSecurityListRequestResponse,
                update_sub_afn=update_sub_afn,
                subscription_cap=fix.ZMCap.SecurityListSubscribe,
                oob_snap_cap=fix.ZMCap.SecurityListOOBSnapshot,
                snap_wait_time_feat="SecurityListSnapshotWaitTime",
                snap_wait_time_default=10,
                get_snapshot_afn=get_snapshot_afn)


    async def TestRequest(self, ident, msg_raw, msg):
        body = msg["Body"]
        if not g.hot_plug_initialized and body.get("_PubReceived"):
            g.hot_plug_initialized = True
            res = {}
            res["Header"] = {"MsgType": fix.MsgType.Heartbeat}
            res["Body"] = {}
            return res
        return (await self._dealer.send_recv_msg(msg_raw, ident=ident))[-1]
        # if not body.get("ZMSendToPub"):
        #     return (await self._dealer.send_recv_msg(msg_raw, ident=ident))[-1]
        # res = {}
        # res["Header"] = {"MsgType": fix.MsgType.Heartbeat}
        # tr_id = body.get("TestReqID")
        # res["Body"] = {}
        # if tr_id:
        #     res["Body"]["TestReqID"] = tr_id
        # pub_res = deepcopy(res)
        # pub_res["Header"]["ZMSendingTime"] = get_timestamp()
        # g.sock_pub.send_string(" " + json.dumps(pub_res))
        # return res


    # async def SecurityListRequest(self, ident, msg_raw, msg):
    #     header = msg["Header"]
    #     body = deepcopy(msg["Body"])
    #     srt = body["SubscriptionRequestType"]
    #     endpoint = header["ZMEndpoint"]
    #     caps = await g.ctl.get_caps(endpoint)
    #     submgr = g.submgrs[endpoint]
    #     if srt == fix.SubscriptionRequestType.Snapshot:
    #         if fix.ZMCap.SecurityListOOBSnapshot in caps:
    #             res = await self._dealer.send_recv_msg(
    #                     msg_raw, ident=ident)
    #             return res[-1]
    #         
    #     if fix.ZMCap.SecurityListSubscribe in 
    #     req_id = body["ZMReqID"]
    #     if srt == fix.SubscriptionRequestType.SnapshotAndUpdates:
    #         if req_id in g.sub_handlers:
    #             raise RejectException(
    #                     "subscription with duplicate ZMReqID exists")
    #         sub_handler = SecurityListSubHandler(msg)
    #         await sub_handler.initialize()
    #         g.sub_handlers[req_id] = sub_handler
    #     if srt == fix.SubscriptionRequestType.Unsubscribe:
    #         if req_id not in g.sub_handlers:
    #             raise RejectException("subscription does not exist")
    #         sub_handler = g.sub_handlers.pop(req_id)
    #     msg = deepcopy(msg)
    #     msg["Body"]["ZMReqID"] = sub_handler.us_req_id
    #     return await self.send_recv_msg(msg, ident=ident)


    async def _handle_msg_2(self, ident, msg_raw, msg, msg_type):
        endpoint = msg["Header"].get("ZMEndpoint")
        if endpoint and endpoint not in g.submgrs:
            g.submgrs[endpoint] = submgr = SubscriptionManager(endpoint)
            await submgr.initialize()
        return await super()._handle_msg_2(ident, msg_raw, msg, msg_type)


###############################################################################


# async def sub_handler():
#     L.info("[sub-handler] running ...")

# async def pub_sock_initializer():
#     elapsed_secs = 0
#     delay = 0.01
#     L.info("[sub_sock_init] sending messages and waiting for confirmation ...")
#     while not g.pub_received:
#         msg = {}
#         msg["Header"] = {"MsgType": fix.MsgType.Heartbeat}
#         msg["Header"]["ZMSendingTime"] = \
#                 int(datetime.utcnow().timestamp() * 1e9)
#         msg["Body"] = {}
#         msg["Body"]["TestReqID"] = "INITIAL_PING"
#         g.sock_pub.send_string(" " + json.dumps(msg))
#         await asyncio.sleep(delay)
#         elapsed_secs += delay
#         if elapsed_secs > 10:
#             L.critical("[sub_sock_init] pub messages not received "
#                        "in time, aborting ...")
#             sys.exit(1)
#     g.pub_initialized.set()


###############################################################################


# async def downstream_pinger():
#     L.debug("downstream pinger running ...")
#     while True:
#         msg = {}
#         msg["Header"] = {"MsgType": fix.MsgType.Heartbeat}
#         msg["Header"]["ZMSendingTime"] = \
#                 int(datetime.utcnow().timestamp() * 1e9)
#         msg["Body"] = {}
#         g.sock_pub.send_string(" " + json.dumps(msg))
#         await asyncio.sleep(g.heartbeat_interval)


###############################################################################


async def pub_init_pinger():
    L.debug("pub init pinger running ...")
    ping_no = 0
    while not g.hot_plug_initialized:
        msg = {}
        msg["Header"] = header = {}
        header["MsgType"] = fix.MsgType.Heartbeat
        header["ZMSendingTime"] = get_timestamp()
        msg["Body"] = {}
        msg["Body"]["_PingNum"] = ping_no
        ping_no += 1
        g.sock_pub.send_string(" " + json.dumps(msg))
        await asyncio.sleep(0.01)


###############################################################################


def parse_args():
    desc = "sessionizer middleware module"
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("ctl_addr_up",
                        help="address of the upstream ctl socket")
    parser.add_argument("ctl_addr_down",
                        help="ctl socket binding address")
    parser.add_argument("pub_addr_up",
                        help="address of the upstream pub socket")
    parser.add_argument("pub_addr_down",
                        help="pub socket binding address")
    parser.add_argument("--heartbeat-interval", type=float, default=1,
                        help="heartbeat interval in seconds")
    parser.add_argument("--hot-plug", action="store_true",
                        help="hot plug mode")
    parser.add_argument("--log-level", default="INFO", help="logging level")
    parser.add_argument("--debug", action="store_true",
                        help="toggle debug mode")
    args = parser.parse_args()
    g.heartbeat_interval = args.heartbeat_interval
    g.debug_mode = args.debug
    g.hot_plug_initialized = not args.hot_plug
    try:
        args.log_level = int(args.log_level)
    except ValueError:
        pass
    return args


def setup_logging(args):
    setup_root_logger(args.log_level)
    disable_logger("parso.python.diff")
    disable_logger("parso.cache")


# def del_if_socket(fn):
#     try:
#         mode = os.stat(fn).st_mode
#     except FileNotFoundError:
#         return
#     if stat.S_ISSOCK(mode):
#         os.unlink(fn)


def init_zmq_sockets(args):
    g.sock_deal = g.ctx.socket(zmq.DEALER)
    g.sock_deal.connect(args.ctl_addr_up)
    # del_if_socket(args.ctl_addr_down)
    g.sock_ctl = g.ctx.socket(zmq.ROUTER)
    g.sock_ctl.bind(args.ctl_addr_down)
    g.sock_sub = g.ctx.socket(zmq.SUB)
    g.sock_sub.connect(args.pub_addr_up)
    #g.sock_sub.subscribe(b"")
    # del_if_socket(args.pub_addr_down)
    g.sock_pub = g.ctx.socket(zmq.PUB)
    g.sock_pub.bind(args.pub_addr_down)
    g.sock_internal_pub = g.ctx.socket(zmq.PUB)
    g.sock_internal_pub.bind(INTERNAL_PUB_ADDR)


def main():
    args = parse_args()
    g.args = args
    setup_logging(args)
    L.debug(f"pyzmq version: {zmq.__version__} (libzmq: {zmq.zmq_version()})")
    if g.debug_mode:
        L.info("debug mode activated")
    init_zmq_sockets(args)
    # g.caps = g.loop.run_until_complete(init_get_capabilities())
    # g.con_features = g.loop.run_until_complete(init_get_connector_features())
    g.ctl = MyController(g.sock_ctl, ReturningDealer(g.ctx, g.sock_deal))
    g.sock_sub.subscribe(g.ctl.session_id.encode())
    g.sub = MySubscriber(g.sock_sub, g.ctl._dealer, name="sub")
    g.pub = Publisher(g.sock_pub)
    tasks = [
        g.ctl.run(),
        # pub_sock_initializer(),
        #delayed(g.sub.run, g.pub_initialized),
        subscriber_receiver(),
        # upstream_pinger(),
        # heartbeater(),
    ]
    if not g.hot_plug_initialized:
        tasks.append(pub_init_pinger())
    tasks = [create_task(coro_obj) for coro_obj in tasks]
    try:
        g.loop.run_until_complete(asyncio.gather(*tasks))
    except KeyboardInterrupt:
        pass
    g.ctx.destroy()


if __name__ == "__main__":
    main()
