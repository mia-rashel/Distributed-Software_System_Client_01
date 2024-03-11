package client;

import java.util.Random;

public class SkierLiftRideEventGenerator {
    private static final int MIN_SKIER_ID = 1;
    private static final int MAX_SKIER_ID = 100000;
    private static final int MIN_RESORT_ID = 1;
    private static final int MAX_RESORT_ID = 10;
    private static final int MIN_LIFT_ID = 1;
    private static final int MAX_LIFT_ID = 40;
    private static final int SEASON_ID = 2022;
    private static final int DAY_ID = 1;
    private static final int MIN_TIME = 1;
    private static final int MAX_TIME = 360;

    private Random random;

    public SkierLiftRideEventGenerator() {
        random = new Random();
    }

    public SkierLiftRideEvent generateEvent() {
        int skierId = getRandomIntInRange(MIN_SKIER_ID, MAX_SKIER_ID);
        int resortId = getRandomIntInRange(MIN_RESORT_ID, MAX_RESORT_ID);
        int liftId = getRandomIntInRange(MIN_LIFT_ID, MAX_LIFT_ID);
        int time = getRandomIntInRange(MIN_TIME, MAX_TIME);

        return new SkierLiftRideEvent(skierId, resortId, liftId, SEASON_ID, DAY_ID, time);
    }

    private int getRandomIntInRange(int min, int max) {
        return random.nextInt(max - min + 1) + min;
    }

    public static class SkierLiftRideEvent {
        private int skierId;
        private int resortId;
        private int liftId;
        private int seasonId;
        private int dayId;
        private int time;

        public SkierLiftRideEvent(int skierId, int resortId, int liftId, int seasonId, int dayId, int time) {
            this.skierId = skierId;
            this.resortId = resortId;
            this.liftId = liftId;
            this.seasonId = seasonId;
            this.dayId = dayId;
            this.time = time;
        }

        // Getters
        public int getSkierId() {
            return skierId;
        }

        public int getResortId() {
            return resortId;
        }

        public int getLiftId() {
            return liftId;
        }

        public int getSeasonId() {
            return seasonId;
        }

        public int getDayId() {
            return dayId;
        }

        public int getTime() {
            return time;
        }
    }
}
