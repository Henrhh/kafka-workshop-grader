package workshop.grader.score;

/** How many points a solve is worth. */
public final class Scoring {

    private Scoring() {
    }

    public static int basePoints(int level) {
        return switch (level) {
            case 1 -> 50;
            case 2 -> 75;
            case 3 -> 100;
            case 4 -> 150;
            case 5, 6 -> 200;
            case 7, 8 -> 250;
            case 9 -> 300;
            case 10 -> 400;
            default -> throw new IllegalArgumentException("No level " + level);
        };
    }

    /**
     * Base points, plus 30%, 20% or 10% for the first three teams to solve the level, minus 10%
     * per failed attempt before the solve (at most 50%), plus any bonus the level grants.
     */
    public static int points(int level, int rank, int failedAttempts, int bonus) {
        int base = basePoints(level);
        int rankBonus = switch (rank) {
            case 1 -> base * 30 / 100;
            case 2 -> base * 20 / 100;
            case 3 -> base * 10 / 100;
            default -> 0;
        };
        int penalty = Math.min(failedAttempts * base / 10, base / 2);
        return base + rankBonus - penalty + bonus;
    }
}
