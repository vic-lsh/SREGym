from enum import StrEnum

# Maximum number of candidate diagnoses an agent may submit in a single
# /submit call. Each candidate is evaluated independently by the LLM judge,
# so this caps worst-case judge cost (N × num_rounds calls).
MAX_DIAGNOSIS_CANDIDATES = 5


class StartProblemResult(StrEnum):

    SUCCESS = "success"
    SKIPPED_KHAOS_REQUIRED = "skipped_khaos_required"
    SKIPPED_SOURCE_DEPLOY_UNSUPPORTED = "skipped_source_deploy_unsupported"
