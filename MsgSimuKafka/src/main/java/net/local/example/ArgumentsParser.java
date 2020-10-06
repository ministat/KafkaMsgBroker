package net.local.example;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class ArgumentsParser {
    @Option(name="-k", aliases="--kafkabrokers", required=true, usage="Specify the kafka server")
    public String kafkabrokers;

    @Option(name="-t", aliases="--kafkatopic", required=true, usage="Specify the kafka topic")
    public String kafkatopic;

    @Option(name="-m", aliases="--mode", usage="producer: 1, consumer: 2, create table: 3, default is 1")
    public int mode = 1;

    @Option(name="-a", aliases="--kudumasters", required=true, usage="Specify the kudu masters")
    public String kuduMaster;

    @Option(name="-n", aliases="--tablename", required=true, usage="Specify the table name")
    public String tableName;

    @Option(name="-p", aliases="--hashpartitions", usage="Specify the number of hash partitions, default is 32")
    public int hashpartitions = 32;

    @Option(name="-r", aliases="--replicas", usage="Specify the number replicas, default is 3")
    public int tablereplicas = 3;

    @Option(name="-d", aliases="--days", usage="Specify the number days, default is 3")
    public int numberofdays = 3;

    @Option(name="-l", aliases="--uselocal", usage="Run spark on local or cluster, default is local")
    public Boolean uselocal = true;

    public boolean parseArgs(final String[] args) {
        final CmdLineParser parser = new CmdLineParser(this);
        if (args.length < 1) {
            parser.printUsage(System.out);
            System.exit(-1);
        }
        boolean ret = true;
        try {
            parser.parseArgument(args);
        } catch (CmdLineException clEx) {
            System.out.println("Error: failed to parse command-line opts: " + clEx);
            ret = false;
        }
        return ret;
    }
}
