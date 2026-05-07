import json
import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import bridge


def cfg(tmp):
    return {
        'feishu': {'bot_name': 'LocalAgent', 'send_as': 'user', 'event_key': 'im.message.receive_v1', 'event_as': 'bot'},
        'security': {
            'max_reply_chars': 3000,
            'command_timeout_seconds': 1,
            'rate_limit_per_user_per_minute': 5,
            'rate_limit_per_user_per_hour': 50,
            'rate_limit_per_chat_per_minute': 20,
            'allowed_chat_ids': [],
            'blocked_open_ids': [],
        },
        'tools': {
            'raw_model_lineage': {'enabled': False, 'script': '/nope'},
            'table_metadata': {'enabled': False},
        },
        'output': {'dir': str(tmp / 'out')},
        'logging': {'dir': str(tmp / 'logs')},
    }


class BridgeUnitTest(unittest.TestCase):
    def test_table_validation_rejects_shell_chars(self):
        with self.assertRaises(bridge.BridgeError):
            bridge.safe_table_name('raw_x;rm -rf /')

    def test_router_rejects_write_intent(self):
        with tempfile.TemporaryDirectory() as d:
            r = bridge.Router(cfg(Path(d)))
            route, _ = r.route('@LocalAgent 删除 raw_x')
            self.assertEqual(route, 'reject_write')

    def test_router_raw(self):
        with tempfile.TemporaryDirectory() as d:
            r = bridge.Router(cfg(Path(d)))
            route, params = r.route('@LocalAgent 查 raw_mysql_db_t_full_1d 对应模型表')
            self.assertEqual(route, 'raw_model_lineage')
            self.assertEqual(params['raw_table'], 'raw_mysql_db_t_full_1d')

    def test_group_requires_mention(self):
        with tempfile.TemporaryDirectory() as d:
            b = bridge.Bridge(cfg(Path(d)), dry_run=True)
            allowed, reason = b.is_allowed_event({'sender_id': 'ou_1', 'chat_id': 'oc_1', 'chat_type': 'group', 'content': '查 raw_x'})
            self.assertFalse(allowed)
            allowed, reason = b.is_allowed_event({'sender_id': 'ou_2', 'chat_id': 'oc_1', 'chat_type': 'group', 'content': '@LocalAgent 查 raw_x'})
            self.assertTrue(allowed)


    def test_router_source_raw_check(self):
        with tempfile.TemporaryDirectory() as d:
            r = bridge.Router(cfg(Path(d)))
            route, params = r.route('@LocalAgent source_db.source_table 是否有接入raw表')
            self.assertEqual(route, 'source_raw_check')
            self.assertEqual(params['source_table'], 'source_db.source_table')

    def test_redaction(self):
        text = bridge.redact_text('DP_SESSION_ID=abc; app_secret: xxx token=yyy')
        self.assertNotIn('abc', text)
        self.assertNotIn('xxx', text)
        self.assertNotIn('yyy', text)


    def test_router_source_hive_check(self):
        with tempfile.TemporaryDirectory() as d:
            r = bridge.Router(cfg(Path(d)))
            route, params = r.route('@LocalAgent source_db.source_table 查询是否有接入hive表?')
            self.assertEqual(route, 'source_raw_check')
            self.assertEqual(params['source_table'], 'source_db.source_table')

    def test_router_generic_query_to_metadata(self):
        with tempfile.TemporaryDirectory() as d:
            r = bridge.Router(cfg(Path(d)))
            route, params = r.route('@LocalAgent 查询 db.table')
            self.assertEqual(route, 'table_metadata')
            self.assertEqual(params['table'], 'db.table')

    def test_spool_long_text(self):
        with tempfile.TemporaryDirectory() as d:
            clipped, path = bridge.maybe_spool('a' * 20, Path(d), 'x', 5)
            self.assertTrue(path)
            self.assertIn('完整内容已保存', clipped)
            self.assertTrue(Path(path).exists())


if __name__ == '__main__':
    unittest.main()
