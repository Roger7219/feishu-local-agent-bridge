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
            'command_timeout_seconds': 2,
            'rate_limit_per_user_per_minute': 5,
            'rate_limit_per_user_per_hour': 50,
            'rate_limit_per_chat_per_minute': 20,
            'allowed_chat_ids': [],
            'blocked_open_ids': [],
            'block_write_intents': False,
        },
        'agent': {
            'enabled': True,
            'command': [sys.executable, '-c', 'import sys; print("agent:" + sys.argv[1])', '{query}'],
            'cwd': str(tmp),
            'env': {},
        },
        'output': {'dir': str(tmp / 'out')},
        'logging': {'dir': str(tmp / 'logs')},
    }


class BridgeUnitTest(unittest.TestCase):
    def test_router_defaults_to_local_agent(self):
        with tempfile.TemporaryDirectory() as d:
            r = bridge.Router(cfg(Path(d)))
            route, params = r.route('@LocalAgent hello world')
            self.assertEqual(route, 'local_agent')
            self.assertEqual(params['query'], 'hello world')

    def test_router_can_reject_write_intent_when_enabled(self):
        with tempfile.TemporaryDirectory() as d:
            c = cfg(Path(d))
            c['security']['block_write_intents'] = True
            r = bridge.Router(c)
            route, _ = r.route('@LocalAgent delete everything')
            self.assertEqual(route, 'reject_write')

    def test_local_agent_runs_configured_command(self):
        with tempfile.TemporaryDirectory() as d:
            r = bridge.Router(cfg(Path(d)))
            result = r.execute('@LocalAgent ping', {'chat_id': 'oc_1', 'sender_id': 'ou_1'})
            self.assertTrue(result.ok)
            self.assertEqual(result.text, 'agent:ping')

    def test_group_requires_mention(self):
        with tempfile.TemporaryDirectory() as d:
            b = bridge.Bridge(cfg(Path(d)), dry_run=True)
            allowed, _ = b.is_allowed_event({'sender_id': 'ou_1', 'chat_id': 'oc_1', 'chat_type': 'group', 'content': 'hello'})
            self.assertFalse(allowed)
            allowed, _ = b.is_allowed_event({'sender_id': 'ou_2', 'chat_id': 'oc_1', 'chat_type': 'group', 'content': '@LocalAgent hello'})
            self.assertTrue(allowed)

    def test_redaction(self):
        text = bridge.redact_text(('access' + '_token') + '=abc; ' + ('app' + '_secret') + ': xxx token=yyy')
        self.assertNotIn('abc', text)
        self.assertNotIn('xxx', text)
        self.assertNotIn('yyy', text)

    def test_spool_long_text(self):
        with tempfile.TemporaryDirectory() as d:
            clipped, path = bridge.maybe_spool('a' * 20, Path(d), 'x', 5)
            self.assertTrue(path)
            self.assertIn('full output saved', clipped)
            self.assertTrue(Path(path).exists())

    def test_render_command_uses_argument_placeholders(self):
        args = bridge.render_command(['python3', 'agent.py', '--query', '{query}', '--chat', '{chat_id}'], 'hello; rm -rf /', {'chat_id': 'oc_1'})
        self.assertEqual(args[-3:], ['hello; rm -rf /', '--chat', 'oc_1'])


if __name__ == '__main__':
    unittest.main()
