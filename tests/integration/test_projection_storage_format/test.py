import time

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/storage_conf.xml", "configs/backups.xml"],
    with_minio=True,
    with_zookeeper=True,
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/storage_conf.xml", "configs/backups.xml"],
    with_minio=True,
    with_zookeeper=True,
    stay_alive=True,
)

# Each known issue links to its PR review comment.
REVIEW = "https://github.com/ClickHouse/ClickHouse/pull/108443#discussion_r"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


def setup_table(name, extra_settings="", n=node):
    n.query(f"DROP TABLE IF EXISTS {name} SYNC")
    n.query("SYSTEM STOP MERGES")  # keep a single, predictable part
    settings = "min_bytes_for_wide_part = 0"
    if extra_settings:
        settings += ", " + extra_settings
    n.query(
        f"""CREATE TABLE {name} (key UInt64, id UInt64, value String,
            PROJECTION p (SELECT key, id, value ORDER BY id))
            ENGINE = MergeTree ORDER BY key SETTINGS {settings}"""
    )
    n.query(
        f"INSERT INTO {name} SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )


def part_dir(name, n=node):  # absolute container path, no trailing slash: .../all_1_1_0
    return (
        n.query(
            f"SELECT path FROM system.parts WHERE table = '{name}' AND active = 1 LIMIT 1"
        )
        .strip()
        .rstrip("/")
    )


def part_name(name, n=node):
    return part_dir(name, n).split("/")[-1]


def proj_query(name, n=node, extra_settings=""):
    settings = "optimize_use_projections = 1"
    if extra_settings:
        settings += ", " + extra_settings
    return n.query(
        f"SELECT count(), sum(key) FROM {name} WHERE id < 200 SETTINGS {settings}"
    ).strip()


def check_table(name, n=node):
    return n.query(
        f"CHECK TABLE {name} SETTINGS check_query_single_value_result = 1"
    ).strip()


def path_exists(p, n=node):
    return (
        n.exec_in_container(
            ["bash", "-c", f"test -e {p} && echo 1 || echo 0"],
            privileged=True,
            user="root",
        ).strip()
        == "1"
    )


def active_parts(name, n=node):
    return n.query(
        f"SELECT count() FROM system.parts WHERE table = '{name}' AND active = 1"
    ).strip()


def active_projection_parts(name, n=node):
    return n.query(
        f"SELECT count() FROM system.projection_parts WHERE table = '{name}' AND active = 1"
    ).strip()


def broken_projection_parts(name, n=node):
    return n.query(
        f"SELECT count() FROM system.projection_parts WHERE table = '{name}' AND is_broken"
    ).strip()


def outdated_parts(name, n=node):
    return n.query(
        f"SELECT count() FROM system.parts WHERE table = '{name}' AND active = 0"
    ).strip()


def wait_for(predicate, timeout=60):
    for _ in range(timeout * 2):
        if predicate():
            return
        time.sleep(0.5)


def test_default_nested_layout():
    setup_table("t_nested")
    p = part_dir("t_nested")
    assert path_exists(f"{p}/p.proj")
    assert not path_exists(f"{p}.p.proj")
    baseline = proj_query("t_nested")
    node.restart_clickhouse()
    assert proj_query("t_nested") == baseline
    assert active_parts("t_nested") == "1"


def test_flat_layout_setting():
    setup_table("t_flat_setting", "projection_storage_format = 'flat'")
    p = part_dir("t_flat_setting")
    # server wrote the projection as a flat sibling, not nested
    assert path_exists(f"{p}.p.proj")
    assert not path_exists(f"{p}/p.proj")
    baseline = proj_query("t_flat_setting")

    # merge keeps the flat layout
    node.query(
        "INSERT INTO t_flat_setting SELECT number, number * 2, toString(number) FROM numbers(1000, 1000)"
    )
    node.query("SYSTEM START MERGES")
    node.query("OPTIMIZE TABLE t_flat_setting FINAL")
    merged = part_dir("t_flat_setting")
    assert path_exists(f"{merged}.p.proj")
    assert not path_exists(f"{merged}/p.proj")
    assert active_parts("t_flat_setting") == "1"
    assert int(active_projection_parts("t_flat_setting")) >= 1
    assert proj_query("t_flat_setting") == baseline

    # survives restart
    node.restart_clickhouse()
    assert active_parts("t_flat_setting") == "1"
    assert proj_query("t_flat_setting") == baseline


def test_flat_layout_after_relocation():
    setup_table("t_flat")
    p = part_dir("t_flat")
    baseline = proj_query("t_flat")
    node.stop_clickhouse()
    node.exec_in_container(
        ["bash", "-c", f"mv {p}/p.proj {p}.p.proj"], privileged=True, user="root"
    )
    node.start_clickhouse()
    assert path_exists(f"{p}.p.proj")
    assert not path_exists(f"{p}/p.proj")
    assert active_parts("t_flat") == "1"
    assert int(active_projection_parts("t_flat")) >= 1
    assert proj_query("t_flat") == baseline


# Issue #1: outdated-part cleanup must remove flat projection siblings too.
# @pytest.mark.xfail(reason=REVIEW + "3472535408", strict=False)
def test_remove_cleans_flat_siblings():
    setup_table("t_rm", "projection_storage_format = 'flat'")
    p = part_dir("t_rm")
    assert path_exists(f"{p}.p.proj")
    node.query(f"ALTER TABLE t_rm DROP PART '{part_name('t_rm')}'")
    wait_for(lambda: not path_exists(f"{p}.p.proj"))
    assert not path_exists(f"{p}.p.proj")


# Issue #2: FREEZE must copy flat projection siblings.
# @pytest.mark.xfail(reason=REVIEW + "3472535412", strict=False)
def test_freeze_includes_flat_projection():
    setup_table("t_freeze", "projection_storage_format = 'flat'")
    node.query("ALTER TABLE t_freeze FREEZE WITH NAME 'flatproj'")
    found = node.exec_in_container(
        ["bash", "-c", "find /var/lib/clickhouse/shadow/flatproj -name '*p.proj' | head -1"],
        privileged=True,
        user="root",
    ).strip()
    assert found != ""


# Issue #2: ATTACH PARTITION FROM (clonePart) must copy flat projection siblings.
# @pytest.mark.xfail(reason=REVIEW + "3472535412", strict=False)
def test_attach_partition_from_clones_flat():
    setup_table("t_src", "projection_storage_format = 'flat'")
    setup_table("t_dst", "projection_storage_format = 'flat'")
    node.query("TRUNCATE TABLE t_dst")
    node.query("ALTER TABLE t_dst ATTACH PARTITION tuple() FROM t_src")
    p = part_dir("t_dst")
    assert path_exists(f"{p}.p.proj")
    assert proj_query("t_dst") == proj_query("t_src")


# Issue #3: a part loaded from disk after restart must keep its flat layout for later operations.
# @pytest.mark.xfail(reason=REVIEW + "3472535414", strict=False)
def test_flat_layout_after_restart_operations():
    setup_table("t_reload", "projection_storage_format = 'flat'")
    baseline = proj_query("t_reload")
    node.restart_clickhouse()
    name = part_name("t_reload")
    node.query(f"ALTER TABLE t_reload DETACH PART '{name}'")
    node.query(f"ALTER TABLE t_reload ATTACH PART '{name}'")
    p = part_dir("t_reload")
    assert path_exists(f"{p}.p.proj")
    assert proj_query("t_reload") == baseline


# Issue #5: replicated fetch must materialize projections in the flat layout.
# @pytest.mark.xfail(reason=REVIEW + "3473479560", strict=False)
def test_replicated_fetch_flat_layout():
    for n, replica in ((node, "1"), (node2, "2")):
        n.query("DROP TABLE IF EXISTS t_repl SYNC")
        n.query("SYSTEM STOP MERGES")
        n.query(
            f"""CREATE TABLE t_repl (key UInt64, id UInt64, value String,
                PROJECTION p (SELECT key, id, value ORDER BY id))
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_repl', '{replica}')
                ORDER BY key
                SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat'"""
        )
    node.query(
        "INSERT INTO t_repl SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )
    node2.query("SYSTEM SYNC REPLICA t_repl")
    p = part_dir("t_repl", node2)
    assert path_exists(f"{p}.p.proj", node2)
    assert proj_query("t_repl", node2) == proj_query("t_repl", node)


# Issue #6: CHECK TABLE must classify an unknown flat projection (left over after
# DROP PROJECTION on a detached part) the same way as a nested one: a projection
# problem ("unexpected projection directories"), not a broken part. The nested-only
# directory scan misses flat siblings, so the stale "p.proj" checksums entry is
# never cleaned and the whole part is reported broken; on ReplicatedMergeTree the
# part-check thread would then detach the part and try to re-fetch it.
def test_check_table_after_dropped_projection():
    for tname, extra in (
        ("t_chk_nested", ""),
        ("t_chk_flat", "projection_storage_format = 'flat'"),
    ):
        setup_table(tname, extra)
        name = part_name(tname)
        node.query(f"ALTER TABLE {tname} DETACH PART '{name}'")
        node.query(f"ALTER TABLE {tname} DROP PROJECTION p")
        node.query(f"ALTER TABLE {tname} ATTACH PART '{name}'")
        result = node.query(
            f"CHECK TABLE {tname} SETTINGS check_query_single_value_result = 0"
        )
        assert "unexpected projection" in result, (tname, result)
        # the data itself must stay readable
        assert node.query(f"SELECT count() FROM {tname}").strip() == "1000"



# Issues #7 and #8: DETACH/ATTACH moves the part under detached/; the flat sibling must follow,
# both on disk and in the in-memory projection storage. After ATTACH PART the projection part's
# root must point at the attached location, not at detached/<part>.p.proj, and the projection
# must stay usable without a silent fallback to the parent part.
# @pytest.mark.xfail(reason=REVIEW + "3473543140", strict=False)
def test_detach_attach_flat_part():
    setup_table("t_da", "projection_storage_format = 'flat'")
    baseline = proj_query("t_da")
    name = part_name("t_da")
    node.query(f"ALTER TABLE t_da DETACH PART '{name}'")
    node.query(f"ALTER TABLE t_da ATTACH PART '{name}'")
    p = part_dir("t_da")
    table_root = p.rsplit("/", 1)[0]
    assert path_exists(f"{p}.p.proj")
    # no projection sibling may be left behind under detached/
    leftover = node.exec_in_container(
        ["bash", "-c", f"find {table_root}/detached -maxdepth 1 -name '*.proj' | wc -l"],
        privileged=True,
        user="root",
    ).strip()
    assert leftover == "0"
    # fail closed: the attached part must serve the projection from its new location
    assert broken_projection_parts("t_da") == "0"
    assert (
        proj_query("t_da", extra_settings="force_optimize_projection = 1") == baseline
    )


# Issue #12: reloading a part must not mark a present flat projection as broken.
# The consistency check resolves the "p.proj" checksums entry by probing a nested
# directory under the part dir, so a flat sibling is reported missing on every
# load (server restart, DETACH/ATTACH TABLE) and the projection is silently
# marked broken; queries then fall back to the parent part.
# @pytest.mark.xfail(reason=REVIEW + "3481208077", strict=False)
def test_flat_projection_not_broken_on_reload():
    setup_table("t_consist", "projection_storage_format = 'flat'")
    baseline = proj_query("t_consist")
    assert broken_projection_parts("t_consist") == "0"
    node.restart_clickhouse()
    assert broken_projection_parts("t_consist") == "0"
    # fail closed: the projection must actually be used, not silently skipped
    assert (
        proj_query("t_consist", extra_settings="force_optimize_projection = 1")
        == baseline
    )


# Issue #9: BACKUP/RESTORE must store and find flat projection data. Projections
# must be serialized under their logical name (<part>/p.proj/...), so backups are
# layout-independent: any version can restore them, and restore of an old backup
# keeps working. With the physical sibling name (<part>/<part>.p.proj/...) restore
# recreates a bogus nested directory and the part loads broken.
def test_backup_restore_flat():
    setup_table("t_bk", "projection_storage_format = 'flat'")
    baseline = proj_query("t_bk")
    node.query("DROP TABLE IF EXISTS t_bk2 SYNC")
    node.exec_in_container(
        ["bash", "-c", "rm -rf /var/lib/clickhouse/backups/t_bk"],
        privileged=True,
        user="root",
    )
    node.query("BACKUP TABLE t_bk TO File('/var/lib/clickhouse/backups/t_bk')")
    physical_dirs = node.exec_in_container(
        ["bash", "-c", "find /var/lib/clickhouse/backups/t_bk -type d -name '*.*.proj' | wc -l"],
        privileged=True,
        user="root",
    ).strip()
    assert physical_dirs == "0"
    logical_dirs = node.exec_in_container(
        ["bash", "-c", "find /var/lib/clickhouse/backups/t_bk -type d -name 'p.proj' | wc -l"],
        privileged=True,
        user="root",
    ).strip()
    assert logical_dirs == "1"
    node.query("RESTORE TABLE t_bk AS t_bk2 FROM File('/var/lib/clickhouse/backups/t_bk')")
    assert node.query("SELECT count() FROM t_bk2").strip() == "1000"
    assert broken_projection_parts("t_bk2") == "0"
    assert proj_query("t_bk2", extra_settings="force_optimize_projection = 1") == baseline
    assert check_table("t_bk2") == "1"


# Issue #11: on zero-copy storage, a mutation must keep blobs hardlinked by flat
# projections. The mutation records hardlinked projection files in the zero-copy
# keep-list; the removal of the source part filters that list by the logical
# projection dir name ("p.proj/..."), so entries recorded under the physical
# sibling name ("<part>.p.proj/...") never match and the shared blobs are deleted
# from under the mutated part.
#
# The scenario needs two replicas: on the mutating replica the shared blobs are
# protected by the local metadata hardlink ref-counts, so the keep-list only
# decides the fate of the blobs on a replica that zero-copy-fetched the mutated
# part (fresh metadata, ref-count 0) and is the last one to unlock the old part.
# node1 executes the mutation (node2's queues are stopped), node2 fetches the
# mutated part, node1 drops the table (releasing its locks), and node2's delayed
# old-part cleanup then decides whether the shared projection blobs survive.
# The mutation must not touch projection columns, otherwise the projection is
# rebuilt instead of hardlinked.
def test_zero_copy_mutation_preserves_flat_projection():
    for n, replica in ((node, "1"), (node2, "2")):
        n.query("DROP TABLE IF EXISTS t_zc SYNC")
        n.query(
            f"""CREATE TABLE t_zc (key UInt64, id UInt64, value String,
                PROJECTION p (SELECT key, id ORDER BY id))
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_zc', '{replica}')
                ORDER BY key
                SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat',
                    storage_policy = 's3', allow_remote_fs_zero_copy_replication = 1,
                    old_parts_lifetime = 20, cleanup_delay_period = 1, max_cleanup_delay_period = 3"""
        )
    node.query(
        "INSERT INTO t_zc SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )
    node2.query("SYSTEM SYNC REPLICA t_zc")
    baseline = proj_query("t_zc")
    assert proj_query("t_zc", node2) == baseline
    p2 = part_dir("t_zc", node2)
    assert path_exists(f"{p2}.p.proj", node2)
    # make node1 execute the mutation and node2 zero-copy-fetch its result
    node2.query("SYSTEM STOP REPLICATION QUEUES t_zc")
    node.query(
        "ALTER TABLE t_zc UPDATE value = concat(value, 'x') WHERE 1 SETTINGS mutations_sync = 1"
    )
    node2.query("SYSTEM START REPLICATION QUEUES t_zc")
    node2.query("SYSTEM SYNC REPLICA t_zc")
    assert active_parts("t_zc", node2) == "1"
    # release node1's zero-copy locks before node2 removes the old part, so
    # node2's removal is the one that decides the fate of the shared blobs
    node.query("DROP TABLE t_zc SYNC")
    wait_for(lambda: outdated_parts("t_zc", node2) == "0")
    assert outdated_parts("t_zc", node2) == "0"
    assert broken_projection_parts("t_zc", node2) == "0"
    assert (
        proj_query("t_zc", node2, extra_settings="force_optimize_projection = 1")
        == baseline
    )
    assert check_table("t_zc", node2) == "1"


def table_path(name, n=node):
    return (
        n.query(f"SELECT data_paths[1] FROM system.tables WHERE name = '{name}'")
        .strip()
        .rstrip("/")
    )


def plant_stale_tmp_dir(stale, n=node):
    """Simulate leftovers of a failed operation: a stale temporary part dir plus its
    flat projection sibling, both containing a marker file. chmod so the server
    (clickhouse user) can remove root-created content."""
    n.exec_in_container(
        [
            "bash",
            "-c",
            f"mkdir -p {stale} {stale}.p.proj"
            f" && touch {stale}/stale_marker.txt {stale}.p.proj/stale_marker.txt"
            f" && chmod -R 777 {stale} {stale}.p.proj",
        ],
        privileged=True,
        user="root",
    )


# Issue #1 (removeRecursive): when an insert reuses the temporary directory name of a
# previously failed insert, the collision cleanup in MergeTreeDataWriter wipes the stale
# tmp_insert_<part> dir via removeRecursive - the stale flat sibling must die with it.
# Otherwise the fresh projection is written into the leftover sibling directory and its
# stale files are published under the live part name.
# @pytest.mark.xfail(reason=REVIEW + "3544856348", strict=False)
def test_stale_tmp_insert_sibling_removed():
    node.query("DROP TABLE IF EXISTS t_ins SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        """CREATE TABLE t_ins (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id, value ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat'"""
    )
    # the first insert into a fresh table writes through tmp_insert_all_1_1_0
    stale = f"{table_path('t_ins')}/tmp_insert_all_1_1_0"
    plant_stale_tmp_dir(stale)
    node.query(
        "INSERT INTO t_ins SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )
    p = part_dir("t_ins")
    assert p.endswith("all_1_1_0")  # the collision branch really ran
    assert path_exists(f"{p}.p.proj")
    # neither the part nor its projection may adopt files of the stale directories
    assert not path_exists(f"{p}/stale_marker.txt")
    assert not path_exists(f"{p}.p.proj/stale_marker.txt")
    # no stale sibling left behind under the temporary name
    assert not path_exists(f"{stale}.p.proj")
    assert broken_projection_parts("t_ins") == "0"
    assert (
        proj_query("t_ins", extra_settings="force_optimize_projection = 1")
        == "100\t4950"
    )
    assert check_table("t_ins") == "1"


def plant_stale_live_sibling(path):
    """A flat projection sibling under a LIVE part name whose parent never committed:
    the residue of a publish that crashed between the sibling and parent renames."""
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"mkdir -p {path} && touch {path}/stale_marker.txt && chmod -R 777 {path}",
        ],
        privileged=True,
        user="root",
    )


# Destination-clearing: a stale flat sibling left at a LIVE part name must not be
# adopted by a later part reusing that name; publishing the name again removes it.
def test_stale_live_sibling_not_adopted():
    node.query("DROP TABLE IF EXISTS t_adopt SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        """CREATE TABLE t_adopt (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id, value ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat',
               materialize_projections_on_insert = 0"""
    )
    plant_stale_live_sibling(f"{table_path('t_adopt')}/all_1_1_0.p.proj")
    node.query(
        "INSERT INTO t_adopt SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )
    p = part_dir("t_adopt")
    assert p.endswith("all_1_1_0")  # the name collision really happened
    node.restart_clickhouse()
    # the part was written without the projection, so nothing may serve one
    assert active_projection_parts("t_adopt") == "0"
    assert not path_exists(f"{p}.p.proj")
    assert node.query("SELECT count() FROM t_adopt").strip() == "1000"
    assert check_table("t_adopt") == "1"


# Destination-clearing: publishing a part WITH a projection over a stale sibling at the
# destination name must clear the leftover instead of failing the sibling rename.
def test_stale_live_sibling_replaced_by_real():
    node.query("DROP TABLE IF EXISTS t_repl_sib SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        """CREATE TABLE t_repl_sib (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id, value ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat'"""
    )
    plant_stale_live_sibling(f"{table_path('t_repl_sib')}/all_1_1_0.p.proj")
    node.query(
        "INSERT INTO t_repl_sib SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )
    p = part_dir("t_repl_sib")
    assert p.endswith("all_1_1_0")
    assert path_exists(f"{p}.p.proj")
    assert not path_exists(f"{p}.p.proj/stale_marker.txt")
    assert (
        proj_query("t_repl_sib", extra_settings="force_optimize_projection = 1")
        == "100\t4950"
    )
    assert check_table("t_repl_sib") == "1"


# DETACH/ATTACH must never leave a mixed state: after DETACH nothing of the part stays
# at live names; after ATTACH the parent and its flat sibling are both live again.
def test_detach_attach_no_mixed_state():
    setup_table("t_mix", "projection_storage_format = 'flat'")
    baseline = proj_query("t_mix")
    live = part_dir("t_mix")
    name = part_name("t_mix")
    table_root = live.rsplit("/", 1)[0]
    node.query(f"ALTER TABLE t_mix DETACH PART '{name}'")
    assert not path_exists(live)
    assert not path_exists(f"{live}.p.proj")
    assert path_exists(f"{table_root}/detached/{name}")
    assert path_exists(f"{table_root}/detached/{name}.p.proj")
    node.query(f"ALTER TABLE t_mix ATTACH PART '{name}'")
    p = part_dir("t_mix")
    assert path_exists(f"{p}.p.proj")
    leftover = node.exec_in_container(
        ["bash", "-c", f"find {table_root}/detached -maxdepth 1 -name '*.proj' | wc -l"],
        privileged=True,
        user="root",
    ).strip()
    assert leftover == "0"
    assert broken_projection_parts("t_mix") == "0"
    assert (
        proj_query("t_mix", extra_settings="force_optimize_projection = 1") == baseline
    )


# A leftover delete_tmp_ pair from an interrupted removal must not block a new removal
# of the same part name, and the flat sibling must be cleaned with it.
def test_delete_tmp_leftovers_cleaned_on_drop():
    setup_table("t_dtmp", "projection_storage_format = 'flat'")
    name = part_name("t_dtmp")
    root = part_dir("t_dtmp").rsplit("/", 1)[0]
    plant_stale_tmp_dir(f"{root}/delete_tmp_{name}")
    node.query(f"ALTER TABLE t_dtmp DROP PART '{name}'")
    wait_for(lambda: not path_exists(f"{root}/{name}"))
    wait_for(lambda: not path_exists(f"{root}/{name}.p.proj"))
    wait_for(lambda: not path_exists(f"{root}/delete_tmp_{name}"))
    wait_for(lambda: not path_exists(f"{root}/delete_tmp_{name}.p.proj"))
    assert not path_exists(f"{root}/delete_tmp_{name}.p.proj")


# DROP PART of a part whose flat sibling vanished must still remove the part completely
# instead of aborting the cleanup halfway.
def test_remove_tolerates_missing_sibling():
    setup_table("t_nosib", "projection_storage_format = 'flat'")
    p = part_dir("t_nosib")
    name = part_name("t_nosib")
    root = p.rsplit("/", 1)[0]
    node.stop_clickhouse()
    node.exec_in_container(
        ["bash", "-c", f"rm -rf {p}.p.proj"], privileged=True, user="root"
    )
    node.start_clickhouse()
    assert node.query("SELECT count() FROM t_nosib").strip() == "1000"
    node.query(f"ALTER TABLE t_nosib DROP PART '{name}'")
    wait_for(lambda: not path_exists(p))
    assert not path_exists(p)
    leftovers = node.exec_in_container(
        ["bash", "-c", f"find {root} -maxdepth 1 -name 'delete_tmp_*' | wc -l"],
        privileged=True,
        user="root",
    ).strip()
    assert leftovers == "0"


# A projection dir present on disk but absent from the manifest means some operation
# diverged from checksums.txt; loading it must leave a warning in the log.
def test_unlisted_projection_warns():
    setup_table("t_warn_src", "projection_storage_format = 'flat'")
    node.query("DROP TABLE IF EXISTS t_warn SYNC")
    node.query(
        """CREATE TABLE t_warn (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id, value ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat',
               materialize_projections_on_insert = 0"""
    )
    node.query(
        "INSERT INTO t_warn SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )
    src_sib = f"{part_dir('t_warn_src')}.p.proj"
    dst_sib = f"{part_dir('t_warn')}.p.proj"
    node.stop_clickhouse()
    node.exec_in_container(
        ["bash", "-c", f"cp -r {src_sib} {dst_sib} && chmod -R 777 {dst_sib}"],
        privileged=True,
        user="root",
    )
    node.start_clickhouse()
    assert node.query("SELECT count() FROM t_warn").strip() == "1000"
    assert node.contains_in_log("loads projection p that is not referenced by its checksums.txt")


# Regenerating a lost manifest must restore projection records: checkDataPart folds them
# only from the loaded projection map, which is empty during the repair inside
# loadChecksums, so the regenerated checksums.txt silently loses every projection and the
# part fails CHECK TABLE forever after.
def test_repair_regenerates_projection_records():
    for tname, extra in (
        ("t_fix_nested", ""),
        ("t_fix_flat", "projection_storage_format = 'flat'"),
    ):
        setup_table(tname, extra)
        baseline = proj_query(tname)
        p = part_dir(tname)
        node.stop_clickhouse()
        node.exec_in_container(
            ["bash", "-c", f"rm {p}/checksums.txt"], privileged=True, user="root"
        )
        node.start_clickhouse()
        assert node.query(f"SELECT count() FROM {tname}").strip() == "1000", tname
        assert broken_projection_parts(tname) == "0", tname
        assert (
            proj_query(tname, extra_settings="force_optimize_projection = 1")
            == baseline
        ), tname
        assert check_table(tname) == "1", tname
        # the regenerated manifest must reference the projection: it has to survive a reload
        node.restart_clickhouse()
        assert active_projection_parts(tname) == "1", tname
        assert check_table(tname) == "1", tname


# Destination-clearing in the detached namespace: a stale sibling under detached/ must
# not fail DETACH of a real part carrying the same name.
def test_publish_over_stale_detached_sibling():
    setup_table("t_det_sib", "projection_storage_format = 'flat'")
    baseline = proj_query("t_det_sib")
    name = part_name("t_det_sib")
    table_root = part_dir("t_det_sib").rsplit("/", 1)[0]
    plant_stale_live_sibling(f"{table_root}/detached/{name}.p.proj")
    node.query(f"ALTER TABLE t_det_sib DETACH PART '{name}'")
    assert path_exists(f"{table_root}/detached/{name}.p.proj")
    assert not path_exists(f"{table_root}/detached/{name}.p.proj/stale_marker.txt")
    node.query(f"ALTER TABLE t_det_sib ATTACH PART '{name}'")
    p = part_dir("t_det_sib")
    assert path_exists(f"{p}.p.proj")
    assert broken_projection_parts("t_det_sib") == "0"
    assert (
        proj_query("t_det_sib", extra_settings="force_optimize_projection = 1")
        == baseline
    )


# Issue #1 (removeSharedRecursive): a retried fetch finds the tmp-fetch_<part> dir of a
# previously failed fetch and wipes it via removeSharedRecursive - the stale flat sibling
# must die with it. Otherwise the retried download materializes the projection into the
# leftover sibling directory, mixing stale files into the fetched part.
# @pytest.mark.xfail(reason=REVIEW + "3534142472", strict=False)
def test_stale_tmp_fetch_sibling_removed():
    for n, replica in ((node, "1"), (node2, "2")):
        n.query("DROP TABLE IF EXISTS t_fetch SYNC")
        n.query("SYSTEM STOP MERGES")
        n.query(
            f"""CREATE TABLE t_fetch (key UInt64, id UInt64, value String,
                PROJECTION p (SELECT key, id, value ORDER BY id))
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_fetch', '{replica}')
                ORDER BY key
                SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat'"""
        )
    node2.query("SYSTEM STOP FETCHES t_fetch")
    node.query(
        "INSERT INTO t_fetch SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )
    name = part_name("t_fetch", node)
    stale = f"{table_path('t_fetch', node2)}/tmp-fetch_{name}"
    plant_stale_tmp_dir(stale, node2)
    node2.query("SYSTEM START FETCHES t_fetch")
    node2.query("SYSTEM SYNC REPLICA t_fetch")
    p2 = part_dir("t_fetch", node2)
    assert path_exists(f"{p2}.p.proj", node2)
    # the fetched part must not adopt files of the stale directories
    assert not path_exists(f"{p2}/stale_marker.txt", node2)
    assert not path_exists(f"{p2}.p.proj/stale_marker.txt", node2)
    assert not path_exists(f"{stale}.p.proj", node2)
    assert broken_projection_parts("t_fetch", node2) == "0"
    assert proj_query(
        "t_fetch", node2, extra_settings="force_optimize_projection = 1"
    ) == proj_query("t_fetch", node)
    assert check_table("t_fetch", node2) == "1"


# Ownership filter (MOVE PART): a flat sibling the part does not own (here: the part was
# written with materialize_projections_on_insert = 0, so it has no projection at all) is
# residue of a failed operation on a same-named part. A cross-disk move clones the part
# via clonePart and must not copy the unowned sibling to the destination.
# https://github.com/ClickHouse/ClickHouse/pull/108443#discussion_r3569019427
def test_move_part_skips_unowned_sibling():
    node.query("DROP TABLE IF EXISTS t_move SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        """CREATE TABLE t_move (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat',
               materialize_projections_on_insert = 0, storage_policy = 'default_and_s3'"""
    )
    node.query(
        "INSERT INTO t_move SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )
    src = part_dir("t_move")
    name = part_name("t_move")
    plant_stale_live_sibling(f"{src}.p.proj")
    node.query(f"ALTER TABLE t_move MOVE PART '{name}' TO DISK 's3'")
    dst = part_dir("t_move")
    assert dst != src  # the part really moved
    assert not path_exists(f"{dst}.p.proj")
    assert not path_exists(f"{dst}/p.proj")
    assert node.contains_in_log(f"Not cloning projection directory {name}.p.proj")
    assert active_projection_parts("t_move") == "0"
    assert node.query("SELECT count() FROM t_move").strip() == "1000"


# Ownership filter (cross-disk ATTACH PARTITION FROM): the destination table lives on a
# different disk, so cloneAndLoadDataPart takes the freezeRemote path - it must apply the
# same owned-projections filter as the same-disk freeze path.
# https://github.com/ClickHouse/ClickHouse/pull/108443#discussion_r3569019441
def test_attach_from_cross_disk_skips_unowned_sibling():
    node.query("DROP TABLE IF EXISTS t_att_src SYNC")
    node.query("DROP TABLE IF EXISTS t_att_dst SYNC")
    node.query("SYSTEM STOP MERGES")
    for tname, policy in (("t_att_src", ""), ("t_att_dst", ", storage_policy = 's3'")):
        node.query(
            f"""CREATE TABLE {tname} (key UInt64, id UInt64, value String,
                PROJECTION p (SELECT key, id ORDER BY id))
                ENGINE = MergeTree ORDER BY key
                SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat',
                    materialize_projections_on_insert = 0{policy}"""
        )
    node.query(
        "INSERT INTO t_att_src SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )
    plant_stale_live_sibling(f"{part_dir('t_att_src')}.p.proj")
    node.query("ALTER TABLE t_att_dst ATTACH PARTITION tuple() FROM t_att_src")
    p = part_dir("t_att_dst")
    assert not path_exists(f"{p}.p.proj")
    assert not path_exists(f"{p}/p.proj")
    assert active_projection_parts("t_att_dst") == "0"
    assert node.query("SELECT count() FROM t_att_dst").strip() == "1000"
    assert check_table("t_att_dst") == "1"


# Ownership filter (mutation): the column-subset mutation path discovers projections from
# disk; a sibling the source part's checksums do not reference must not be hardlinked into
# the mutated part. The mutated column is not used by the projection, so an owned
# projection would be hardlinked - the unowned one must be skipped instead.
def test_mutation_skips_unowned_sibling():
    node.query("DROP TABLE IF EXISTS t_mut SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        """CREATE TABLE t_mut (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat',
               materialize_projections_on_insert = 0"""
    )
    node.query(
        "INSERT INTO t_mut SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )
    src = part_dir("t_mut")
    plant_stale_live_sibling(f"{src}.p.proj")
    node.query(
        "ALTER TABLE t_mut UPDATE value = concat(value, 'x') WHERE 1 SETTINGS mutations_sync = 1"
    )
    p = part_dir("t_mut")
    assert p != src  # the mutation produced a new part
    assert not path_exists(f"{p}.p.proj")
    assert not path_exists(f"{p}/p.proj")
    assert active_projection_parts("t_mut") == "0"
    assert node.query("SELECT count() FROM t_mut").strip() == "1000"


# Manifest repair: regenerating a lost checksums.txt restores records only for projections
# declared in the table metadata. An undeclared projection directory (here: q.proj, a valid
# projection dir copied under a name the table never had) must not be legitimized by the
# regenerated manifest.
def test_repair_skips_undeclared_projection_dir():
    setup_table("t_undecl", "projection_storage_format = 'flat'")
    baseline = proj_query("t_undecl")
    p = part_dir("t_undecl")
    node.stop_clickhouse()
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"cp -r {p}.p.proj {p}.q.proj && chmod -R 777 {p}.q.proj && rm {p}/checksums.txt",
        ],
        privileged=True,
        user="root",
    )
    node.start_clickhouse()
    assert node.query("SELECT count() FROM t_undecl").strip() == "1000"
    assert node.contains_in_log(
        "Not restoring checksums record for projection directory q.proj"
    )
    # the regenerated manifest must reference the declared projection and not the undeclared one
    manifest = node.exec_in_container(
        ["bash", "-c", f"grep -ao '[pq]\\.proj' {p}/checksums.txt | sort -u"],
        privileged=True,
        user="root",
    )
    assert "p.proj" in manifest
    assert "q.proj" not in manifest
    assert broken_projection_parts("t_undecl") == "0"
    assert (
        proj_query("t_undecl", extra_settings="force_optimize_projection = 1")
        == baseline
    )


# Detached surface: after DETACH PART on a FLAT table, system.detached_parts must show one
# entry (no junk row for the sibling) whose bytes_on_disk includes the sibling, and
# DROP DETACHED PART must remove the sibling too.
# https://github.com/ClickHouse/ClickHouse/pull/108443#discussion_r3569019447
def test_detached_surface_flat_sibling():
    setup_table("t_det_surf", "projection_storage_format = 'flat'")
    name = part_name("t_det_surf")
    table_root = part_dir("t_det_surf").rsplit("/", 1)[0]
    node.query(f"ALTER TABLE t_det_surf DETACH PART '{name}'")
    rows = node.query(
        "SELECT name FROM system.detached_parts WHERE table = 't_det_surf'"
    ).strip()
    assert rows == name  # exactly one entry, no row for the sibling
    bytes_on_disk = int(
        node.query(
            f"SELECT bytes_on_disk FROM system.detached_parts WHERE table = 't_det_surf' AND name = '{name}'"
        ).strip()
    )

    def files_size(path):
        return int(
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    f"find {path} -type f -printf '%s\\n' | awk '{{s+=$1}} END {{print s+0}}'",
                ],
                privileged=True,
                user="root",
            ).strip()
        )

    parent_size = files_size(f"{table_root}/detached/{name}")
    sibling_size = files_size(f"{table_root}/detached/{name}.p.proj")
    assert sibling_size > 0
    assert bytes_on_disk >= parent_size + sibling_size
    node.query(
        f"ALTER TABLE t_det_surf DROP DETACHED PART '{name}' SETTINGS allow_drop_detached = 1"
    )
    assert not path_exists(f"{table_root}/detached/{name}")
    assert not path_exists(f"{table_root}/detached/{name}.p.proj")
    leftovers = node.exec_in_container(
        ["bash", "-c", f"find {table_root}/detached -maxdepth 1 -name '*.proj' | wc -l"],
        privileged=True,
        user="root",
    ).strip()
    assert leftovers == "0"
