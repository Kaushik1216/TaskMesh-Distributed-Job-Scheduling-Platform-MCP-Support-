package com.taskmesh.common.enums;

/**
 * Determines how a workflow reacts when a node job fails.
 *
 * FAIL_FAST: Cancel all remaining nodes immediately. The workflow is marked FAILED.
 * CONTINUE:  Skip dependents of the failed node, but let independent branches proceed.
 *            The workflow is marked FAILED only after all runnable nodes finish.
 */
public enum FailurePolicy {
    FAIL_FAST,
    CONTINUE
}
