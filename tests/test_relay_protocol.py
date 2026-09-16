import io
import unittest

from relay_protocol import (
    DEFAULT_CHUNK,
    MAGIC,
    ProtocolError,
    agent_supports_sparse,
    crc32,
    encode_chunk,
    encode_hole,
    read_chunk,
    read_frame,
)
from relay_protocol import (
    decode_handshake,
    encode_handshake,
    issue_token,
    verify_token,
)


class ChunkFramingTest(unittest.TestCase):
    def test_encode_hole_round_trips_through_read_frame(self):
        stream = io.BytesIO(encode_hole(4 * 1024 * 1024, 4096))

        self.assertEqual(read_frame(stream), ("hole", 4 * 1024 * 1024, 4096, b""))

    def test_read_frame_parses_data_frame(self):
        stream = io.BytesIO(encode_chunk(1024, b"payload"))

        self.assertEqual(read_frame(stream), ("data", 1024, 7, b"payload"))

    def test_read_frame_reports_end_of_stream(self):
        self.assertEqual(read_frame(io.BytesIO(b"")), ("eof", -1, 0, b""))

    def test_read_chunk_rejects_hole_frame(self):
        with self.assertRaises(ProtocolError):
            read_chunk(io.BytesIO(encode_hole(0, 4096)))

    def test_encode_hole_rejects_bad_length(self):
        with self.assertRaises(ProtocolError):
            encode_hole(0, 0)

    def test_agent_supports_sparse_compares_numeric_segments(self):
        self.assertFalse(agent_supports_sparse("1.0.0"))
        self.assertTrue(agent_supports_sparse("1.1.0"))
        self.assertTrue(agent_supports_sparse("1.10.0"))
        self.assertFalse(agent_supports_sparse(""))
        self.assertFalse(agent_supports_sparse("bogus"))

    def test_roundtrip_preserves_offset_and_payload(self):
        frame = encode_chunk(4096, b"hello-block")

        stream = io.BytesIO(frame)
        offset, payload = read_chunk(stream)

        self.assertEqual(offset, 4096)
        self.assertEqual(payload, b"hello-block")

    def test_read_chunk_reports_end_of_stream(self):
        self.assertEqual(read_chunk(io.BytesIO(b"")), (-1, b""))

    def test_read_chunk_rejects_crc_mismatch(self):
        frame = bytearray(encode_chunk(0, b"payload"))
        frame[-1] ^= 0xFF

        with self.assertRaises(ProtocolError):
            read_chunk(io.BytesIO(bytes(frame)))

    def test_read_chunk_rejects_bad_magic(self):
        frame = bytearray(encode_chunk(0, b"payload"))
        frame[0:4] = b"XXXX"

        with self.assertRaises(ProtocolError):
            read_chunk(io.BytesIO(bytes(frame)))

    def test_read_chunk_rejects_truncated_payload(self):
        frame = encode_chunk(0, b"payload")

        with self.assertRaises(ProtocolError):
            read_chunk(io.BytesIO(frame[:-2]))

    def test_encode_chunk_rejects_payload_over_limit(self):
        with self.assertRaises(ProtocolError):
            encode_chunk(0, b"x" * (DEFAULT_CHUNK * 4))

    def test_crc32_matches_zlib(self):
        import zlib

        self.assertEqual(crc32(b"abc"), zlib.crc32(b"abc") & 0xFFFFFFFF)

    def test_magic_is_four_bytes(self):
        self.assertEqual(len(MAGIC), 4)


class TokenTest(unittest.TestCase):
    SECRET = b"unit-test-secret"

    def test_verify_token_returns_job_and_role(self):
        token = issue_token(self.SECRET, "job-1", "source", now=1000, ttl=300)

        self.assertEqual(
            verify_token(self.SECRET, token, now=1100), ("job-1", "source")
        )

    def test_verify_token_rejects_expired(self):
        token = issue_token(self.SECRET, "job-1", "target", now=1000, ttl=60)

        with self.assertRaises(ProtocolError):
            verify_token(self.SECRET, token, now=1100)

    def test_verify_token_rejects_tampered_role(self):
        token = issue_token(self.SECRET, "job-1", "target", now=1000, ttl=300)
        forged = token.replace("|target|", "|source|")

        with self.assertRaises(ProtocolError):
            verify_token(self.SECRET, forged, now=1100)

    def test_verify_token_rejects_other_secret(self):
        token = issue_token(self.SECRET, "job-1", "source", now=1000, ttl=300)

        with self.assertRaises(ProtocolError):
            verify_token(b"other-secret", token, now=1100)

    def test_verify_token_rejects_malformed(self):
        with self.assertRaises(ProtocolError):
            verify_token(self.SECRET, "not-a-token", now=1100)


class HandshakeTest(unittest.TestCase):
    def test_handshake_roundtrip(self):
        line = encode_handshake("ticket-1", 1024, 2048)

        self.assertEqual(
            decode_handshake(line),
            {"ticket": "ticket-1", "offset": 1024, "length": 2048},
        )

    def test_decode_handshake_rejects_missing_ticket(self):
        with self.assertRaises(ProtocolError):
            decode_handshake(b'{"offset": 0, "length": 10}\n')

    def test_decode_handshake_rejects_negative_offset(self):
        with self.assertRaises(ProtocolError):
            decode_handshake(b'{"ticket": "t", "offset": -1, "length": 10}\n')
