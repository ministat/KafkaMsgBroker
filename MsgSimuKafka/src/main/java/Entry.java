public class Entry {
    public static void main(final String[] args) throws Exception {
        final ArgumentsParser parser = new ArgumentsParser();
        if (!parser.parseArgs(args)) {
            return;
        }
        switch (parser.mode) {
            case 1:
                FIXGenerator.FIXGenerate(parser.kafkabrokers, parser.kafkatopic);
                break;
            case 2:
                break;
            default:
                System.out.println("Unrecognized mode: " + parser.mode);
        }
    }
}
