import fcntl
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'core'))
import pipeline as p


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name) / 'control'
        self.source = Path(self.tmp.name) / 'source'
        self.source.mkdir()
        self.conv = 'a' * 32

    def tearDown(self):
        self.tmp.cleanup()

    def save(self, records, conversation=None):
        conv = conversation or self.conv
        path = self.source / (conv + '.json')
        path.write_text(json.dumps({'conversationId': conv, 'messages': records}))
        return path

    def msg(self, sid='1', text='synthetic fixture', ts=1792000000):
        return {'serverId': sid, 'id': sid, 'content': text, 'createTime': ts, 'type': 'text'}

    def ingest(self):
        return p.ingest(self.root, self.source)

    def test_replay_and_source_unchanged(self):
        path = self.save([self.msg()])
        before = path.read_bytes()
        self.assertEqual(self.ingest()['new'], 1)
        again = self.ingest()
        self.assertEqual((again['new'], again['updated'], again['change_count']), (0, 0, 1))
        self.assertEqual(before, path.read_bytes())

    def test_update_and_revert_are_events_not_duplicates(self):
        self.save([self.msg()]); self.ingest()
        self.save([self.msg(text='updated')]); self.assertEqual(self.ingest()['updated'], 1)
        self.save([self.msg()]); result = self.ingest()
        self.assertEqual((result['updated'], result['message_count'], result['change_count']), (1, 1, 3))

    def test_same_text_different_ids_or_conversations_survives(self):
        self.save([self.msg('1'), self.msg('2')])
        self.save([self.msg('1')], 'b' * 32)
        self.assertEqual(self.ingest()['message_count'], 3)

    def test_malformed_file_rolls_back_messages_and_cursor(self):
        path = self.save([self.msg(), {'createTime': 'bad'}])
        bad = self.ingest()
        self.assertEqual((bad['state'], bad['message_count'], bad['input_file_count']), ('partial', 0, 0))
        self.save([self.msg(), self.msg('2')])
        self.assertEqual(self.ingest()['new'], 2)

    def test_late_message_summarized_by_ingestion_sequence(self):
        self.save([self.msg()]); self.ingest()
        p.summarize(self.root)
        self.save([self.msg(), self.msg('2', ts=100)])
        self.ingest()
        summary = p.summarize(self.root)
        self.assertEqual((summary['records'], summary['through_seq']), (1, 2))
        self.assertFalse(p.summarize(self.root)['summary_created'])

    def test_crash_after_db_commit_republishes_without_duplication(self):
        self.save([self.msg()]); self.ingest()
        with patch.object(p, 'atomic', side_effect=OSError('synthetic disk error')):
            with self.assertRaises(OSError): p.summarize(self.root)
        self.assertFalse(p.summarize(self.root)['summary_created'])
        result = p.publish(self.root)
        self.assertEqual(result['summaries'], 1)
        self.assertTrue((self.root / 'summaries/vm-1-1.json').is_file())

    def test_single_writer_and_read_does_not_ack_dot(self):
        self.save([self.msg()]); self.ingest()
        with p.writer(self.root):
            with self.assertRaises(BlockingIOError): self.ingest()
        result = p.read(self.root, metadata_only=True)
        self.assertFalse(result['dot_verified'])
        self.assertNotIn('message', result['records'][0])
        db = p.connect(self.root, True)
        self.assertEqual(db.execute('SELECT count(*) FROM consumers').fetchone()[0], 0)
        with self.assertRaises(Exception): db.execute("INSERT INTO consumers VALUES('dot',1)")
        db.close()

    def test_symlink_rejected(self):
        target = Path(self.tmp.name) / 'elsewhere.json'
        target.write_text('{}')
        (self.source / (self.conv + '.json')).symlink_to(target)
        self.assertEqual(self.ingest()['state'], 'partial')

    def test_missing_source_is_not_zero_success(self):
        with self.assertRaises(ValueError): p.ingest(self.root, self.source / 'missing')


if __name__ == '__main__':
    unittest.main()
