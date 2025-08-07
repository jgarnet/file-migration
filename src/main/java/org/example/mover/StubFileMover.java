package org.example.mover;

import org.example.exception.MigrationException;
import org.example.file.SourceFile;

import java.util.Random;
import java.util.UUID;

public class StubFileMover implements FileMover {
    private final static Random random = new Random();
    private final static int FAIL_PERCENTAGE = 5;

    @Override
    public String move(SourceFile sourceFile) throws MigrationException {
        double rand = random.nextDouble() * 100;
        if (rand >= FAIL_PERCENTAGE) {
            return "s3://some-bucket/" + UUID.randomUUID();
        }
        throw new MigrationException("Failed to move file");
    }
}
