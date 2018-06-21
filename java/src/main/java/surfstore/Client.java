package surfstore;

import java.io.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.Vector;
import java.util.HashMap;
import java.util.Arrays;
import java.lang.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.WriteResult;
import surfstore.SurfStoreBasic.SimpleAnswer;
import surfstore.SurfStoreBasic.NodeList;
import surfstore.SurfStoreBasic.Index;

public final class Client 
{
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel metadataChannel;
    private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;

    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    private final ConfigReader config;
    protected HashMap<String, byte[]> clientMap;

    private String m_functionName;
    private String m_filePath;
    private String m_fileName;

    public Client(ConfigReader config) 
    {
        this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(config.getLeaderNum())).usePlaintext(true).build();
        this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);

        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort()).usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        this.config = config;
        clientMap = new HashMap<String, byte[]>();
    }

    public void setFunctionName(String name)
    {
        m_functionName = name;
    }

    public void setFilePath(String name)
    {
        m_filePath = name;
    }

    public void setFileName(String name)
    {
        m_fileName = name;
    }

    public String getFunctionName()
    {
        return m_functionName;
    }

    public String getFilePath()
    {
        return m_filePath;
    }

    public String getFileName()
    {
        return m_fileName;
    }

    public void shutdown() throws InterruptedException 
    {
        metadataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    private  Block stringToBlock(String s)
    {
        Block.Builder builder = Block.newBuilder();
        try
        {
            builder.setData(ByteString.copyFrom(s, "UTF-8"));
        } 
        catch(UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
        builder.setHash(this.generateHash(s));
        return builder.build(); 
    }
    
    public String generateHash(String s)
    {
        MessageDigest digest = null;
        try
        {
            digest = MessageDigest.getInstance("SHA-256");
        }
        catch(NoSuchAlgorithmException e)
        {
            e.printStackTrace();
        }
        byte[] hash = digest.digest(s.getBytes(StandardCharsets.UTF_8));
        String encoded = Base64.getEncoder().encodeToString(hash);
        return encoded;
    }

	private void go() 
    {
		switch(this.m_functionName)
        {
            case "getversion":
                int ver = this.getVersion(m_fileName);
                System.out.println(ver);
                break;
            case "delete":
                this.deleteFile(m_fileName);
                break;
            case "download":
                System.out.println("Downloads");
                this.downloadFile(m_fileName, m_filePath);
                break;
            case "upload":
                this.updating(m_fileName);
                break;
            default:
                break;
        }
	}

    public int getVersion(String fileName)
    {
        File file = new File(fileName);
        String o_fileName = file.getName();
        FileInfo myFile = metadataStub.getVersion(FileInfo.newBuilder().setFilename(o_fileName).build());
        return myFile.getVersion();
    }

    public void updating(String filepath)
    {
        boolean continueBool = true;
        while(continueBool)
        {
            int result = this.updateFile(filepath);
            if(result == 0 || result == 2)
            {
                continueBool = false;
                if(result == 0)
                {
                    System.out.println("NOT_LEADER");
                }
                else
                {
                    System.out.println("OK");
                }
            }
        }
    }

    public int updateFile(String filePath) //The absolute path should be passed.
    {
        File file = new File(filePath);
        String fileName = file.getName();
        FileInfo sendInfo = FileInfo.newBuilder().setFilename(fileName).build();
        FileInfo recvInfo = metadataStub.readFile(sendInfo);
        int fileVersion = recvInfo.getVersion();
        ++fileVersion;
        Vector<String> hashList = generateHashList(filePath);
        FileInfo.Builder finalInfo = FileInfo.newBuilder();
        finalInfo.setFilename(fileName);
        finalInfo.setVersion(fileVersion);
        for(int i=0; i < hashList.size(); ++i)
        {
            finalInfo.addBlocklist(hashList.get(i));
        }
        WriteResult result = metadataStub.modifyFile(finalInfo.build());
        if(result.getResult() == WriteResult.Result.NOT_LEADER)
        {
            return 0;
        }
        if(result.getResult() == WriteResult.Result.OLD_VERSION)
        {
            return 1;
        }
        if(result.getResult() == WriteResult.Result.MISSING_BLOCKS)
        {
            for(int i=0; i < result.getMissingBlocksCount(); ++i) // fill up missing blocks
            {
                //System.out.println("Adding Blocks");
                Block.Builder myBlock = Block.newBuilder();
                myBlock.setData(ByteString.copyFrom(clientMap.get(result.getMissingBlocks(i))));
                myBlock.setHash(result.getMissingBlocks(i));
                blockStub.storeBlock(myBlock.build());
            }
            return 1;
        }
        return 2;
    }

    public void downloadFile(String fileName, String filePath)
    {
        FileInfo myFile = FileInfo.newBuilder().setFilename(fileName).build();
        HashMap<String, byte[]> localMap = new HashMap<String, byte[]>();
        File folder = new File(filePath);
        String absolute_name = filePath + "/" + fileName;
        File[] files = folder.listFiles();
        for(File file : files) 
        {
            try
            {
                String file_name = filePath + "/" + file.getName(); //file_name is path + filename
                InputStream m_stream = new FileInputStream(file_name);
                File m_file = new File(file_name);
                int length = (int)m_file.length();
                int total = 0;
                byte[] data = new byte[Math.min(length - total, 4096)];
                try
                {
                    int bytesRead = m_stream.read(data, 0, Math.min(length - total, 4096));
                    total += bytesRead;
                    String s = new String(data);
                    String hashVal = generateHash(s);
                    localMap.put(hashVal, data); // data is byte[]
                    while(total < length)
                    {
                        byte[] m_data = new byte[Math.min(length - total, 4096)];
                        bytesRead = m_stream.read(m_data, 0, Math.min(length - total, 4096));
                        total += bytesRead;
                        String m_s = new String(m_data);
                        hashVal = generateHash(m_s);
                        localMap.put(hashVal, m_data);
                    }
                }
                catch(IOException e)
                {
                    System.out.println("IOException");
                }
            }
            catch(FileNotFoundException e)
            {
                System.out.println("FileNotFoundException");
            }
        } // Gathering all blocks in a current directory is done!
        FileInfo newFileInfo = metadataStub.readFile(myFile); // newFileInfo contains fileinfo
        if(newFileInfo.getVersion() == 0 || newFileInfo.getBlocklistCount() == 0 || newFileInfo.getBlocklist(0) == "0")
        {
            System.out.println("Not Found");
        }
        else
        {
            try
            {
                FileOutputStream outputStream = new FileOutputStream(absolute_name);
                for(int i=0; i < newFileInfo.getBlocklistCount(); ++i) // getting every hash value
                {
                    byte[] myBlock;
                    if(!localMap.containsKey(newFileInfo.getBlocklist(i))) // Should get a block from the block store
                    {
                        Block t_block = Block.newBuilder().setHash(newFileInfo.getBlocklist(i)).build();
                        Block s_block = blockStub.getBlock(t_block);
                        myBlock = s_block.getData().toByteArray();
                    }
                    else
                    {
                        myBlock = localMap.get(newFileInfo.getBlocklist(i));
                    }
                    try
                    {
                        outputStream.write(myBlock);
                    }
                    catch(IOException e)
                    {
                        System.out.println("IOException");
                    }
                }
            }
            catch(FileNotFoundException e)
            {
                System.out.println("FileNotFoundException");
            }
            //outputStream.close();
        }
    }

    public void deleteFile(String fileName)
    {
        boolean go = true;
        while(go)
        {
            FileInfo sendInfo = FileInfo.newBuilder().setFilename(fileName).build();
            FileInfo myInfo = metadataStub.readFile(sendInfo);
            int version = myInfo.getVersion();
            ++version;
            WriteResult recvResult = metadataStub.deleteFile(FileInfo.newBuilder().setVersion(version).setFilename(fileName).build());
            if(recvResult.getResult() != WriteResult.Result.OLD_VERSION)
            {
                go = false;
                if(recvResult.getResult() == WriteResult.Result.MISSING_BLOCKS)
                {
                    System.out.println("Not Found");
                }
                else if(recvResult.getResult() == WriteResult.Result.OK)
                {
                    System.out.println("OK");
                }
                else
                {
                    System.out.println("NOT LEADER");
                }
            }
        }

    }

    public Vector<String> generateHashList(String filePath)
    {
        Vector<String> retVec = new Vector<String>();
        try
        {
            InputStream m_stream = new FileInputStream(filePath);
            File file = new File(filePath);
            int length = (int)file.length();
            int total = 0;
            byte[] data = new byte[Math.min(length - total, 4096)];
            try
            {
                int bytesRead = m_stream.read(data, 0, Math.min(length - total, 4096));
                total += bytesRead;
                String s = new String(data);
                String hashVal = generateHash(s);
                clientMap.put(hashVal, data);
                retVec.add(hashVal);
                while(total < length)
                {
                    byte[] m_data = new byte[Math.min(length - total, 4096)];
                    bytesRead = m_stream.read(m_data, 0, Math.min(length - total, 4096));
                    total += bytesRead;
                    String m_s = new String(m_data);
                    hashVal = generateHash(m_s);
                    clientMap.put(hashVal, m_data);
                    retVec.add(hashVal);
                }
            }
            catch(IOException e)
            {
                System.out.println("IOException in generateHashList");
            }
        }
        catch(FileNotFoundException e)
        {
            System.out.println("FileNotFoundException in generateHashList");
        }
        return retVec;
    }
	/*
	 * TODO: Add command line handling here
	 */
    private static Namespace parseArgs(String[] args) 
    {
        ArgumentParser parser = ArgumentParsers.newFor("Client").build().description("Client for SurfStore");
        parser.addArgument("config_file").type(String.class).help("Path to configuration file");
        parser.addArgument("method_name").type(String.class).help("METHOD_NAME");
        parser.addArgument("file_name").type(String.class).help("FILE_NAME");
        if(3 < args.length)
        {
            parser.addArgument("file_path").type(String.class).help("FILE_PATH");
        }
        Namespace res = null;
        try 
        {
            res = parser.parseArgs(args);
        } 
        catch(ArgumentParserException e)
        {
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
        //System.out.println(c_args.getString("path"));
        ConfigReader config = new ConfigReader(configf);
        //System.out.println("Client Constructor");
        Client client = new Client(config);
        //System.out.println("Leader Number: "+config.getLeaderNum());
        client.setFunctionName(c_args.getString("method_name"));
        client.setFileName(c_args.getString("file_name"));
        if(3 < args.length)
        {
            client.setFilePath(c_args.getString("file_path"));
        }
        try 
        {
        	client.go();
        } 
        finally 
        {
            client.shutdown();
        }
    }

}
