#!/bin/bash

set -x

RESUME_LAST=${RESUME_LAST:-true}
RESUME_FROM=${RESUME_FROM:-""}
PARALLEL=${PARALLEL:-1}
MODEL=${MODEL:-vertex-ai-gemini-2.5-flash}
AGENT=${AGENT:-gemini_cli}
ENABLE_RESOURCE_THROTTLING=${ENABLE_RESOURCE_THROTTLING:-false}
SEQUENCE_LEN=${SEQUENCE_LEN:-0}
SEQUENCE_SEED=${SEQUENCE_SEED:-42}

# summary features
ENABLE_SUMMARY=${ENABLE_SUMMARY:-true}
NO_INJECT_SUMMARY=${NO_INJECT_SUMMARY:-false}
SEED_SUMMARY=${SEED_SUMMARY:-""}
SUMMARY_MODEL=${SUMMARY_MODEL:-"vertex-ai-gemini-2.5-flash"}
JUDGE_MODEL=${JUDGE_MODEL:-"vertex-ai-gemini-2.5-pro"}

ARGS=""
if [ -n "$RESUME_FROM" ]; then
    ARGS="--resume-from $RESUME_FROM"
elif [ "$RESUME_LAST" = true ]; then
    ARGS="--resume-last"
fi

if [ "$ENABLE_SUMMARY" = true ]; then
    ARGS="$ARGS --enable-summary"
fi

if [ "$NO_INJECT_SUMMARY" = true ]; then
    ARGS="$ARGS --no-inject-kb"
fi

if [ -n "$SEED_SUMMARY" ]; then
    ARGS="$ARGS --seed-summary $SEED_SUMMARY"
fi

if [ -n "$SUMMARY_MODEL" ]; then
    ARGS="$ARGS --kb-model $SUMMARY_MODEL"
fi

if [ -n "$JUDGE_MODEL" ]; then
    ARGS="$ARGS --judge-model $JUDGE_MODEL"
fi

if [ "$SEQUENCE_LEN" -gt 0 ] 2>/dev/null; then
    ARGS="$ARGS --sequence-len $SEQUENCE_LEN --sequence-seed $SEQUENCE_SEED"
fi

SREGYM_PRELOAD_INFRA_IMAGES=0

time \
    SREGYM_ENABLE_RESOURCE_THROTTLING=$ENABLE_RESOURCE_THROTTLING \
    SREGYM_PRELOAD_INFRA_IMAGES=$SREGYM_PRELOAD_INFRA_IMAGES \
    SREGYM_PROGRESS_MODE=rich \
    uv run main.py --agent $AGENT --model $MODEL --parallel $PARALLEL $ARGS
