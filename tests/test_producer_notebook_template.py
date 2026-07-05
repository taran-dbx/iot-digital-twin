"""Guard tests for the industry-first Kafka producer generator.

The producer notebook is generated client-side in TypeScript
(``client/src/lib/data-producer-notebook.ts``). As with the consumer guards,
these tests read the template source and assert the spike-VERIFIED patterns
(spec §1.8, §1.15) are present:

  * kafka-python producer with ``security_protocol="SASL_SSL"`` /
    ``sasl_mechanism="OAUTHBEARER"``.
  * MSK-IAM token provider subclassing ``AbstractTokenProvider`` ->
    ``MSKAuthTokenProvider.generate_auth_token(...)``.
  * AWS creds hydrated from the UC service credential via
    ``get_credentials().get_frozen_credentials()`` (do NOT upgrade boto3).
  * realtime = unbatched per-record flush; micro-batch = batched flush.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

PRODUCER_TS = (
    Path(__file__).resolve().parent.parent
    / "client" / "src" / "lib" / "data-producer-notebook.ts"
)

pytestmark = pytest.mark.skipif(
    not PRODUCER_TS.exists(),
    reason="producer template not present (client/ gitignored)",
)


def _producer_section() -> str:
    """Isolate the industry Kafka producer section from the legacy generators."""
    text = PRODUCER_TS.read_text()
    m = re.search(
        r"// Industry-first Kafka producer.*?// Zerobus producer",
        text,
        re.DOTALL,
    )
    assert m is not None, "could not locate industry Kafka producer section"
    return m.group(0)


class TestOAuthBearerAuth:
    def test_sasl_ssl_oauthbearer(self) -> None:
        section = _producer_section()
        assert 'security_protocol="SASL_SSL"' in section
        assert 'sasl_mechanism="OAUTHBEARER"' in section

    def test_msk_iam_token_provider(self) -> None:
        section = _producer_section()
        assert "AbstractTokenProvider" in section
        assert "MSKAuthTokenProvider.generate_auth_token" in section
        assert "sasl_oauth_token_provider=MSKTokenProvider()" in section

    def test_frozen_credentials_from_service_credential(self) -> None:
        section = _producer_section()
        assert "getServiceCredentialsProvider" in section
        assert "get_credentials().get_frozen_credentials()" in section

    def test_does_not_upgrade_boto3(self) -> None:
        section = _producer_section()
        assert "--upgrade boto3" not in section
        assert "aws-msk-iam-sasl-signer-python" in section


class TestGenerationSemantics:
    def test_realtime_is_unbatched_microbatch_is_batched(self) -> None:
        section = _producer_section()
        # linger_ms=0 + batch_size=1 for realtime; batched otherwise.
        assert "const lingerMs = isRealtime ? 0 : 100;" in section
        assert "const batchSize = isRealtime ? 1 : (config.batchSize ?? 50);" in section

    def test_realtime_flushes_each_record(self) -> None:
        section = _producer_section()
        # Real-time send loop flushes per record (unbatched).
        assert "send + flush each record individually (unbatched)" in section
        # Micro-batch send loop uses a single flush per batch.
        assert "a single flush" in section

    def test_uses_simulator_and_records_per_sec(self) -> None:
        section = _producer_section()
        assert "from iot_simulator import Simulator" in section
        assert "rec/s" in section

    def test_does_not_create_topic(self) -> None:
        # Topic is created up-front by the provisioning engine (§1.8).
        section = _producer_section()
        assert "create_topics" not in section
        assert "NewTopic" not in section
