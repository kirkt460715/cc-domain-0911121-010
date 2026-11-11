import argparse
import json
import sys
from .types import obj, decode, canonical, fail, ChainError
from .events.exchange import read_file
from .service import SupplyChain


def dispatch(app, request):
    obj(request, required={"action"})
    values = dict(request)
    action = values.pop("action")
    if action == "register":
        return app.consumer.register(**values)
    if action == "submit":
        return app.consumer.submit(values["event"])
    if action == "drain":
        return app.consumer.drain(**values)
    if action == "event":
        return app.consumer.status(values["id"])
    if action == "pending":
        return app.consumer.pending()
    if action == "skip":
        return app.consumer.skip(values["id"], values["reason"])
    if action == "gaps":
        return app.consumer.gaps(values["source"])
    if action == "lot":
        return app.inventory.lot(values["id"])
    if action == "inventory":
        return app.inventory.list(**values)
    if action == "locations":
        return app.inventory.by_location()
    if action == "supplier":
        return app.inventory.supplier_exposure(values["id"])
    if action == "shipments":
        return app.inventory.shipments(**values)
    if action == "yields":
        return app.inventory.process_yields()
    if action == "operations":
        return app.inventory.operations(values["lot"])
    if action == "trace":
        return app.trace.subgraph(**values)
    if action == "path":
        return app.trace.path(values["ancestor"], values["descendant"])
    if action == "contributions":
        return app.trace.root_contributions(values["lot"])
    if action == "impact":
        return app.trace.impact(values["lot"])
    if action == "conservation":
        return app.trace.conservation_report()
    if action == "topology":
        return app.trace.topology()
    if action == "correct":
        return app.projections.correct(**values)
    if action == "verify_rebuild":
        return app.projections.verify_rebuild()
    if action == "checkpoint":
        return app.projections.checkpoint(values["id"])
    if action == "compare_checkpoint":
        return app.projections.compare_checkpoint(values["id"])
    if action == "recall_open":
        return app.recalls.open(**values)
    if action == "recall":
        return app.recalls.get(values["id"])
    if action == "recall_acknowledge":
        return app.recalls.acknowledge(**values)
    if action == "recall_refresh":
        return app.recalls.refresh(**values)
    if action == "recall_close":
        return app.recalls.close(**values)
    if action == "certificate":
        return app.recalls.certificate(**values)
    if action == "revoke_certificate":
        return app.recalls.revoke_certificate(**values)
    if action == "stocktake_open":
        return app.stocktakes.open(**values)
    if action == "stocktake_count":
        return app.stocktakes.count(**values)
    if action == "stocktake_diff":
        return app.stocktakes.differences(values["id"])
    if action == "stocktake_close":
        return app.stocktakes.close(**values)
    if action == "stocktake_rebase":
        return app.stocktakes.rebase(**values)
    if action == "import":
        return app.exchange.import_jsonl(read_file(values["file"]))
    if action == "export":
        return app.exchange.write_source(**values)
    if action == "offsets":
        return {"csv": app.exchange.offsets_csv()}
    if action == "compare_source":
        return app.exchange.compare_source(**values)
    if action == "check":
        return app.check()
    if action == "backup":
        return {"path": app.journal.backup(values["path"])}
    if action == "inspection_spec":
        return app.quality.specification(values["specification"])
    if action == "inspection_open":
        return app.quality.open(**values)
    if action == "inspection_sample":
        return app.quality.sample(**values)
    if action == "inspection_assess":
        return app.quality.evaluate(values["id"])
    if action == "inspection_disposition":
        return app.quality.disposition(**values)
    if action == "supplier_quality":
        return app.quality.supplier_summary(values["supplier"])
    if action == "packet":
        return app.packets.create(values["id"], values["shipment"])
    if action == "packet_export":
        return app.packets.export(values["id"], values["path"])
    if action == "packet_verify":
        from .views.packets import verify_packet

        return verify_packet(values["path"])
    if action == "pick_plan":
        return app.picking.plan(**values)
    if action == "pick_dispatch":
        return app.picking.dispatch(**values)
    if action == "pick_cancel":
        return app.picking.cancel(**values)
    if action == "pick_freshness":
        return app.picking.freshness(values["id"])
    if action == "pick_queue":
        return app.picking.queue(**values)
    fail("input", "unknown action", action=action)


def main(argv=None):
    parser = argparse.ArgumentParser(prog="chainledger")
    parser.add_argument("--db", default="chainledger.sqlite3")
    parser.add_argument("command", choices=["run", "demo", "check", "pending"])
    parser.add_argument("file", nargs="?")
    args = parser.parse_args(argv)
    try:
        if args.command == "demo":
            from .demonstration import run

            result = run()
        else:
            with SupplyChain(args.db) as app:
                if args.command == "run":
                    if not args.file:
                        fail("input", "run requires action JSON file")
                    result = dispatch(app, decode(read_file(args.file)))
                elif args.command == "check":
                    result = app.check()
                else:
                    result = app.consumer.pending()
    except ChainError as exc:
        print(canonical({"error": exc.as_dict()}), file=sys.stderr)
        return 2
    except (OSError, KeyError, TypeError) as exc:
        print(
            canonical({"error": {"code": "request", "message": str(exc)}}),
            file=sys.stderr,
        )
        return 3
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0
