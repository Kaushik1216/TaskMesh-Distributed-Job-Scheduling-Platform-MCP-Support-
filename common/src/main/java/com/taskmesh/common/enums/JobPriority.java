package com.taskmesh.common.enums;

public enum JobPriority {
    CRITICAL(4),
    HIGH(3),
    NORMAL(2),
    LOW(1);

    private final int level;

    JobPriority(int level) {
        this.level = level;
    }

    public int getLevel() {
        return level;
    }
}
