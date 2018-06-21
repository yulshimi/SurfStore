package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.HashMap;
import java.util.Vector;
import java.util.Timer;
import java.util.TimerTask;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.WriteResult;
import surfstore.SurfStoreBasic.SimpleAnswer;
import surfstore.SurfStoreBasic.NodeList;
import surfstore.SurfStoreBasic.Index;

public final class MetadataStore 
{
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());
    public static ManagedChannel blockChannel;
    public static BlockStoreGrpc.BlockStoreBlockingStub blockStub;
    public static ManagedChannel metadataChannel;
    public static MetadataStoreGrpc.MetadataStoreBlockingStub leaderStub;
    public static ManagedChannel metadataChannel_1;
    public static MetadataStoreGrpc.MetadataStoreBlockingStub followerStub_1;
    public static ManagedChannel metadataChannel_2;
    public static MetadataStoreGrpc.MetadataStoreBlockingStub followerStub_2;
    public static boolean multiServer;

    protected Server server;
	protected ConfigReader config;
    public static boolean m_leader;
    public static boolean m_crash;
    public MetadataStore(ConfigReader config) 
    {
        m_crash = false;
    	this.config = config;
        //blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort()).usePlaintext(true).build();
        //blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);
        if(m_leader)
        {
            multiServer = false;
            blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort()).usePlaintext(true).build();
            blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);
            if(1 < config.getNumMetadataServers())
            {
                multiServer = true;
                metadataChannel_1 = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(2)).usePlaintext(true).build();
                followerStub_1 = MetadataStoreGrpc.newBlockingStub(metadataChannel_1);
                metadataChannel_2 = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(3)).usePlaintext(true).build();
                followerStub_2 = MetadataStoreGrpc.newBlockingStub(metadataChannel_2);
            }
        }
        else
        {
            multiServer = false;
            metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(config.getLeaderNum())).usePlaintext(true).build();
            leaderStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);
        }
	}

	private void start(int port, int numThreads) throws IOException 
    {
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl())
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                MetadataStore.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() 
    {
        if (server != null) 
        {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException 
    {
        if(server != null) 
        {
            server.awaitTermination();
        }
    }

    private static Namespace parseArgs(String[] args) 
    {
        ArgumentParser parser = ArgumentParsers.newFor("MetadataStore").build()
                .description("MetadataStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("-n", "--number").type(Integer.class).setDefault(1)
                .help("Set which number this server is");
        parser.addArgument("-t", "--threads").type(Integer.class).setDefault(10)
                .help("Maximum number of concurrent threads");

        Namespace res = null;
        try 
        {
            res = parser.parseArgs(args);
        } 
        catch (ArgumentParserException e){
            parser.handleError(e);
        }
        return res;
    }

    public static void main(String[] args) throws Exception 
    {
        Namespace c_args = parseArgs(args);
        if(c_args == null)
        {
            throw new RuntimeException("Argument parsing failed");
        }
        
        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);

        if(c_args.getInt("number") > config.getNumMetadataServers()) 
        {
            throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));
        }
        System.out.println("Number: " + c_args.getInt("number"));
        System.out.println("Leader: " + config.getLeaderNum());
        if(c_args.getInt("number") == config.getLeaderNum())
        {
            System.out.println("True");
            m_leader = true;
        }
        else
        {
            System.out.println("False");
            m_leader = false;
        }

        final MetadataStore server = new MetadataStore(config);
        if(m_leader == true && multiServer == true)
        {
            Timer t = new Timer();
            t.schedule(new TimerTask(){
                @Override
                public void run() {
                    followerStub_1.restore(Empty.newBuilder().build());
                    followerStub_2.restore(Empty.newBuilder().build());
                }
            }, 0, 5000);
        }
        server.start(config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"));
        server.blockUntilShutdown();
    }

    static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase 
    {
        protected HashMap<String, Vector<String>> fileStorage;
        protected HashMap<String, Integer> versionStorage;
        protected Vector<String> logVector;

        public MetadataStoreImpl()
        {
            super();
            fileStorage = new HashMap<String, Vector<String>>();
            versionStorage = new HashMap<String, Integer>();
            logVector = new Vector<String>();
        }

        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver) 
        {
            System.out.println("I am in ping meta");
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        //@Override
        public void readFile(FileInfo req, final StreamObserver<FileInfo> responseObserver) //don't need to care about version
        {
            FileInfo.Builder response = FileInfo.newBuilder();
            if(!m_leader)
            {
                if(versionStorage.containsKey(req.getFilename()))
                {
                    FileInfo res = response.setFilename(req.getFilename()).setVersion(versionStorage.get(req.getFilename())).build();
                    responseObserver.onNext(res);
                    responseObserver.onCompleted();
                }
                else
                {
                    FileInfo res = response.setFilename(req.getFilename()).setVersion(0).build();
                    responseObserver.onNext(res);
                    responseObserver.onCompleted();
                }
            }
            if(multiServer) // 2 Phase commit
            {
                SimpleAnswer follower1 = followerStub_1.isCrashed(Empty.newBuilder().build());
                SimpleAnswer follower2 = followerStub_2.isCrashed(Empty.newBuilder().build());
                if(follower1.getAnswer() == true && follower2.getAnswer() == true)
                {
                    FileInfo res = response.setFilename(req.getFilename()).build(); 
                    responseObserver.onNext(res);
                    responseObserver.onCompleted();
                }
            }
            this.logVector.add(req.getFilename());
            if(fileStorage.containsKey(req.getFilename())) // if a file exists
            {
                response.setVersion(versionStorage.get(req.getFilename()));
                response.setFilename((req.getFilename()));
                if(0 < fileStorage.get(req.getFilename()).size())
                {
                    for(int i=0; i < fileStorage.get(req.getFilename()).size(); ++i)
                    {
                        response.addBlocklist(fileStorage.get(req.getFilename()).get(i));
                    }
                }
            }
            else
            {
                fileStorage.put(req.getFilename(), new Vector<String>());
                fileStorage.get(req.getFilename()).add("0");
                versionStorage.put(req.getFilename(), 0);
                response.setFilename((req.getFilename()));
                response.setVersion(0);
            }
            if(multiServer)
            {
                FileInfo followInfo = FileInfo.newBuilder().addAllBlocklist(fileStorage.get(req.getFilename())).setVersion(versionStorage.get(req.getFilename())).setFilename(req.getFilename()).build();
                followerStub_1.append(followInfo);
                followerStub_2.append(followInfo);
            }
            FileInfo res = response.build();
            responseObserver.onNext(res);
            responseObserver.onCompleted();
        }

        //@Override
        public void deleteFile(FileInfo req, final StreamObserver<WriteResult> responseObserver) 
        {
            WriteResult.Builder response = WriteResult.newBuilder();
            if(!m_leader)
            {
                WriteResult res = response.setResult(WriteResult.Result.NOT_LEADER).build();
                responseObserver.onNext(res);
                responseObserver.onCompleted();
            }
            if(multiServer)
            {
                SimpleAnswer follower1 = followerStub_1.isCrashed(Empty.newBuilder().build());
                SimpleAnswer follower2 = followerStub_2.isCrashed(Empty.newBuilder().build());
                if(follower1.getAnswer() == true && follower2.getAnswer() == true)
                {
                    WriteResult res = response.setResult(WriteResult.Result.NOT_LEADER).build(); // Both follwers crashed not appropirate error message
                    responseObserver.onNext(res);
                    responseObserver.onCompleted();
                }
            }
            this.logVector.add(req.getFilename());
            if(fileStorage.containsKey(req.getFilename()))
            {
                if(0 < fileStorage.get(req.getFilename()).size() && fileStorage.get(req.getFilename()).get(0) != "0")
                {
                    if(req.getVersion() <= versionStorage.get(req.getFilename()))
                    {
                        response.setResult(WriteResult.Result.OLD_VERSION);
                        response.setCurrentVersion(versionStorage.get(req.getFilename()));
                    }
                    else
                    {
                        versionStorage.put(req.getFilename(), req.getVersion());
                        fileStorage.put(req.getFilename(), new Vector<String>());
                        fileStorage.get(req.getFilename()).add("0");
                        response.setResult(WriteResult.Result.OK);
                        response.setCurrentVersion(req.getVersion());
                    }
                }
                else
                {
                    response.setCurrentVersion(versionStorage.get(req.getFilename()));
                    response.setResult(WriteResult.Result.MISSING_BLOCKS); //actually not found
                }
            }
            else
            {
                response.setResult(WriteResult.Result.MISSING_BLOCKS); // When a file does not exist    
            }
            if(multiServer)
            {
                FileInfo followInfo = FileInfo.newBuilder().addAllBlocklist(fileStorage.get(req.getFilename())).setVersion(versionStorage.get(req.getFilename())).setFilename(req.getFilename()).build();
                followerStub_1.append(followInfo);
                followerStub_2.append(followInfo);
            }
            WriteResult res = response.build();
            responseObserver.onNext(res);
            responseObserver.onCompleted();
        }

        public void printLog(Empty req, final StreamObserver<Empty> responseObserver)
        {
            Empty res = Empty.newBuilder().build();
            for(int i=0; i < logVector.size(); ++i)
            {
                System.out.println(logVector.get(i));
            }
            responseObserver.onNext(res);
            responseObserver.onCompleted();
        } 

        public void getVersion(FileInfo req, final StreamObserver<FileInfo> responseObserver)
        {
            responseObserver.onNext(FileInfo.newBuilder().setVersion(versionStorage.get(req.getFilename())).setFilename(req.getFilename()).build());
            responseObserver.onCompleted();
        }

        public void isLeader(Empty req, final StreamObserver<SimpleAnswer> responseObserver)
        {
            responseObserver.onNext(SimpleAnswer.newBuilder().setAnswer(m_leader).build());
            responseObserver.onCompleted();
        }

        public void isCrashed(Empty req, final StreamObserver<SimpleAnswer> responseObserver)
        {
            responseObserver.onNext(SimpleAnswer.newBuilder().setAnswer(m_crash).build());
            responseObserver.onCompleted();
        }

        public void crash(Empty req, final StreamObserver<Empty> responseObserver)
        {
            if(!m_leader)
            {
                m_crash = true;
            }
            responseObserver.onNext(Empty.newBuilder().build());
            responseObserver.onCompleted();
        }


        //@Override
        public void modifyFile(FileInfo req, final StreamObserver<WriteResult> responseObserver) 
        {
            WriteResult.Builder response = WriteResult.newBuilder();
            Boolean error = false;
            if(!m_leader)
            {
                WriteResult res = response.setResult(WriteResult.Result.NOT_LEADER).build();
                responseObserver.onNext(res);
                responseObserver.onCompleted();
            }
            if(multiServer)
            {
                SimpleAnswer follower1 = followerStub_1.isCrashed(Empty.newBuilder().build());
                SimpleAnswer follower2 = followerStub_2.isCrashed(Empty.newBuilder().build());
                if(follower1.getAnswer() == true && follower2.getAnswer() == true)
                {
                    WriteResult res = response.setResult(WriteResult.Result.NOT_LEADER).build(); // Both follwers crashed not appropirate error message
                    responseObserver.onNext(res);
                    responseObserver.onCompleted();
                }
            }

            if(!versionStorage.containsKey(req.getFilename())) 
            {
                fileStorage.put(req.getFilename(), new Vector<String>());
                fileStorage.get(req.getFilename()).add("0");
                versionStorage.put(req.getFilename(), 0);
            }

            this.logVector.add(req.getFilename());

            if(req.getVersion() <= versionStorage.get(req.getFilename())) // NullPointerException
            {
                response.setResult(WriteResult.Result.OLD_VERSION);
                response.setCurrentVersion(versionStorage.get(req.getFilename()));
                error = true;
            }
            else
            {   
                Boolean isMissing = false; 
                for(int i=0; i < req.getBlocklistCount(); ++i)
                {
                    Block j_block = Block.newBuilder().setHash(req.getBlocklist(i)).build();
                    SimpleAnswer s_answer = blockStub.hasBlock(j_block);
                    if(!s_answer.getAnswer())
                    {
                        response.addMissingBlocks(req.getBlocklist(i));
                        isMissing = true;
                    }
                }
                if(isMissing)
                {
                    response.setResult(WriteResult.Result.MISSING_BLOCKS);
                    response.setCurrentVersion(versionStorage.get(req.getFilename()));
                    error = true;
                }
            }
            if(!error) // It means there is no error
            {
                fileStorage.put(req.getFilename(), new Vector<String>());
                for(int i=0;  i < req.getBlocklistCount(); ++i)
                {
                    fileStorage.get(req.getFilename()).add(req.getBlocklist(i));
                }
                versionStorage.put(req.getFilename(), req.getVersion());
                response.setResult(WriteResult.Result.OK);
                response.setCurrentVersion(req.getVersion());
            }
            if(multiServer)
            {
                FileInfo followInfo = FileInfo.newBuilder().addAllBlocklist(fileStorage.get(req.getFilename())).setVersion(versionStorage.get(req.getFilename())).setFilename(req.getFilename()).build();
                followerStub_1.append(followInfo);
                followerStub_2.append(followInfo);
            }
            WriteResult res = response.build();
            responseObserver.onNext(res);
            responseObserver.onCompleted();
        }

        public void append(FileInfo req, final StreamObserver<Empty> responseObserver)
        {
            if(!m_crash)
            {
                logVector.add(req.getFilename());
                fileStorage.put(req.getFilename(), new Vector<String>());
                for(int i=0; i < req.getBlocklistCount(); ++i)
                {
                    fileStorage.get(req.getFilename()).add(req.getBlocklist(i));
                }
                versionStorage.put(req.getFilename(), req.getVersion());
            }
            responseObserver.onNext(Empty.newBuilder().build());
            responseObserver.onCompleted();
        }

        public void restore(Empty req, final StreamObserver<Empty> responseObserver) // on a follower side
        {
            if(m_crash == false)
            {
                responseObserver.onNext(Empty.newBuilder().build());
                responseObserver.onCompleted();
            }
            Empty response = Empty.newBuilder().build();
            Index vectorSize = leaderStub.getVectorSize(response);
            HashMap<String, Integer> tempMap = new HashMap<String, Integer>();
            for(int i=this.logVector.size(); i < vectorSize.getIndex(); ++i) // Make the log vector up-to-date
            {
                Index.Builder myIndex = Index.newBuilder();
                FileInfo tempInfo = leaderStub.getLogName(myIndex.setIndex(i).build());
                logVector.add(tempInfo.getFilename());
                tempMap.put(tempInfo.getFilename(), 0);
            }
            for(String key : tempMap.keySet()) // Make the hash list up-to-date
            {
                FileInfo sendInfo = FileInfo.newBuilder().setFilename(key).build();
                FileInfo tempInfo = leaderStub.sendUpdate(sendInfo);
                fileStorage.put(key, new Vector<String>());
                for(int i=0; i < tempInfo.getBlocklistCount(); ++i)
                {
                    fileStorage.get(key).add(tempInfo.getBlocklist(i));
                }
                versionStorage.put(key, tempInfo.getVersion());
            }
            m_crash = false;
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        public void sendUpdate(FileInfo req, final StreamObserver<FileInfo> responseObserver) // on a leader side 
        {
            FileInfo.Builder response = FileInfo.newBuilder();
            response.setFilename(req.getFilename());
            response.setVersion(versionStorage.get(req.getFilename()));
            response.addAllBlocklist(fileStorage.get(req.getFilename()));
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        }

        public void getLogName(Index req, final StreamObserver<FileInfo> responseObserver) // sending a corresponding file name
        {
            FileInfo.Builder response = FileInfo.newBuilder();
            response.setFilename(logVector.get(req.getIndex()));
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        }

        public void getVectorSize(Empty req, final StreamObserver<Index> responseObserver) // on a follower side
        {
            Index.Builder response = Index.newBuilder();
            response.setIndex(logVector.size());
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        }
    }
}























