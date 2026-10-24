package workshop.grader.kafka;

import java.security.SecureRandom;

/** Passwords and JAAS lines for SCRAM users. */
public final class Credentials {

    private static final String ALPHABET = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789";
    private static final SecureRandom RANDOM = new SecureRandom();

    private Credentials() {
    }

    public static String newPassword() {
        StringBuilder password = new StringBuilder(24);
        for (int i = 0; i < 24; i++) {
            password.append(ALPHABET.charAt(RANDOM.nextInt(ALPHABET.length())));
        }
        return password.toString();
    }

    public static String jaasConfig(String username, String password) {
        if (username.contains("\"") || password.contains("\"")) {
            throw new IllegalArgumentException("Usernames and passwords cannot contain quotes");
        }
        return "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";"
                .formatted(username, password);
    }
}
