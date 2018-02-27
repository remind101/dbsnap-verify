import unittest

import mock

import json

from dbsnap_verify.state_doc import (
    DocToObject,
    StateDoc,
    DbsnapVerifyStateDoc,
)


JSON_STATE_DOC = """{
  "states": [
    {
      "state": "wait",
      "timestamp": 1519821752.33388
    },
    {
      "state": "restore",
      "timestamp": 1519821861.627925
    }
  ],
  "state_doc_name": "test",
  "state_doc_path": null,
  "state_doc_bucket": "bucket-to-hold-state-documents"
}"""

DICT_STATE_DOC = json.loads(JSON_STATE_DOC)

mock_none = mock.Mock(return_value=None)
mock_json_state_doc = mock.Mock(return_value=JSON_STATE_DOC)
mock_dict_state_doc = mock.Mock(return_value=DICT_STATE_DOC)


class TestDocToObject(unittest.TestCase):

    def test_no_args(self):
        o = DocToObject()
        self.assertEqual(o.__dict__, {})

    def test_invalid_attribute(self):
        o = DocToObject()
        with self.assertRaises(AttributeError):
            o.a

    def test_two_args_from_dictionary(self):
        o = DocToObject({"a":1, "b":2})
        self.assertEqual(o.a, 1)
        self.assertEqual(o.b, 2)

    def test_to_json(self):
        o1 = DocToObject({"a":1, "b":2})
        self.assertEqual(o1.to_json, '{\n  "a": 1,\n  "b": 2\n}')
        o2 = DocToObject({"b":2, "a":1})
        self.assertEqual(o2.to_json, '{\n  "b": 2,\n  "a": 1\n}')


class TestStateDoc(unittest.TestCase):

    def setUp(self):
        self.name = "test"
        self.bucket = "bucket-to-hold-state-documents"
        self.states = [
            {"state" : "wait", "timestamp" : 1519821752.33388},
            {"state" : "restore", "timestamp" : 1519821861.627925},
        ]
        self.state_doc = StateDoc(
            name=self.name,
            states=self.states,
            state_doc_bucket=self.bucket
        )
        self.json_state_doc = JSON_STATE_DOC
        self.dict_state_doc = DICT_STATE_DOC

    def test_minimal_args(self):
        state_doc = StateDoc(name=self.name)
        self.assertEqual(state_doc.state_doc_name, self.name)
        self.assertEqual(state_doc.state_doc_bucket, None)
        self.assertEqual(state_doc.state_doc_path, None)
        self.assertEqual(state_doc.current_state, None)
        self.assertEqual(state_doc.states, [])

    def test_with_bucket(self):
        state_doc = StateDoc(name=self.name, state_doc_bucket=self.bucket)
        self.assertEqual(state_doc.state_doc_name, self.name)
        self.assertEqual(state_doc.state_doc_bucket, self.bucket)
        self.assertEqual(state_doc.state_doc_path, None)
        self.assertEqual(state_doc.current_state, None)
        self.assertEqual(state_doc.states, [])

    def test_current_state(self):
        self.assertEqual(self.state_doc.current_state, "restore")

    def test_to_json(self):
        self.assertEqual(self.state_doc.to_json, self.json_state_doc)

    def test_state_doc_s3_key(self):
        self.assertEqual(self.state_doc.state_doc_s3_key, "state-doc-test.json")

    @mock.patch(
        'dbsnap_verify.state_doc.StateDoc._save_state_doc_in_s3',
        mock_none
    )
    def test_transition(self):
        self.state_doc.transition_state("modify")
        self.assertEqual(self.state_doc.current_state, "modify")
        self.assertEqual(len(self.state_doc.states), 3)

    @mock.patch(
        'dbsnap_verify.state_doc.StateDoc._save_state_doc_in_s3',
        mock_none
    )
    def test_transition_2(self):
        self.state_doc.transition_state("modify")
        self.state_doc.transition_state("verify")
        self.assertEqual(self.state_doc.current_state, "verify")
        self.assertEqual(len(self.state_doc.states), 4)

    @mock.patch(
        'dbsnap_verify.state_doc.StateDoc._load_state_doc_from_s3',
        mock_dict_state_doc
    )
    def test_load_from_s3(self):
        state_doc = StateDoc(self.name, state_doc_bucket=self.bucket)
        state_doc.load()
        self.assertEqual(len(self.state_doc.states), 2)

class TestDictDbsnapVerifyStateDoc(unittest.TestCase):

    def setUp(self):
        self.state_doc = DbsnapVerifyStateDoc(
            database = "prod-test-db",
            database_subnet_ids = "subnet-1111,subnet-2222,subnet-3333,subnet-4444",
            database_security_group_ids = "sg-0123456789",
            state_doc_bucket = "bucket-to-hold-state-documents",
            snapshot_region = "us-west-1",
        )
        self.json_state_doc = self.state_doc.to_json

    def test_minimal_args(self):
        self.assertEqual(
            self.state_doc.database,
            "prod-test-db"
        )
        self.assertEqual(
            self.state_doc.database_subnet_ids,
            "subnet-1111,subnet-2222,subnet-3333,subnet-4444"
        )
        self.assertEqual(
            self.state_doc.database_security_group_ids,
            "sg-0123456789"
        )
        self.assertEqual(
            self.state_doc.state_doc_bucket,
            "bucket-to-hold-state-documents"
        )
        self.assertEqual(
            self.state_doc.snapshot_region,
            "us-west-1"
        )
        self.assertEqual(
            self.state_doc.state_doc_path,
            None
        )
        with self.assertRaises(AttributeError):
            self.state_doc.invalid_attribute

    @mock.patch(
        'dbsnap_verify.state_doc.StateDoc._save_state_doc_in_s3',
        mock_none
    )
    def test_clean(self):
        self.state_doc.states = range(0,1000)
        self.state_doc.tmp_password = "test-password"
        self.assertEqual(len(self.state_doc.states), 1000)
        self.state_doc.save()
        self.state_doc.clean()
        self.assertEqual(len(self.state_doc.states), 100)
        self.assertEqual(self.state_doc.tmp_password, None)


