package workshop.grader.levels;

import java.util.Optional;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Small JSON helpers for graders. */
final class Json {

    private Json() {
    }

    /** Parses a team's output, or empty if it is not JSON. */
    static Optional<JsonNode> parse(JsonMapper json, String value) {
        if (value == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(json.readTree(value));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    static String shorten(String value) {
        if (value == null) {
            return "null";
        }
        return value.length() <= 120 ? value : value.substring(0, 117) + "...";
    }
}
