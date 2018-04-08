import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.ImmutableList;

public class HdfsService {
	// service to hdfs
public static void main(String[] args) {
	System.out.println(new HdfsService().createDir("testDir"));;
}
    private FileSystem getCorSys() {
        FileSystem coreSys = null;
        Configuration conf = new Configuration();
        try {
            return FileSystem.get(URI.create("hdfs://192.168.109.130:9000"), conf,"hadoop");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return coreSys;
    }

    //创建目录
    public boolean createDir(String path){
        FileSystem coreSys=getCorSys();
        try {
            return coreSys.mkdirs(new Path(path));
        } catch (IOException e) {
           e.printStackTrace();
           return false;
        }finally {
            try {
                coreSys.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //修改目录\文件
    public boolean renameDir(String oldPath,String newPath){
        FileSystem coreSys=getCorSys();
        try {
            return coreSys.rename(new Path(oldPath),new Path(newPath));
        } catch (IOException e) {
            return false;
        }finally {
            try {
                coreSys.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //删除目录\文件
    public boolean delDir(String path){
        FileSystem coreSys=getCorSys();
        try {
            return coreSys.delete(new Path(path),true);
        } catch (IOException e) {
            return false;
        }finally {
            try {
                coreSys.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //目录和文件信息
    public List<IOVFile> listFile(String path) throws IOException {
        FileSystem coreSys=getCorSys();
        Path p = new Path(path);
        FileStatus[] files = coreSys.listStatus(p);
        ImmutableList.Builder<IOVFile> builder = ImmutableList.builder();
        FileStatus[] var5 = files;
        int var6 = files.length;

        for(int var7 = 0; var7 < var6; ++var7) {
            FileStatus file = var5[var7];
            builder.add(new IOVFile(file));
        }

        return builder.build();
    }

    //文件上传
    public boolean uoloadFile(String srcPath,String desPath){
        FileSystem coreSys=getCorSys();
        try {
            if(coreSys.isDirectory(new Path(desPath))){
                coreSys.copyFromLocalFile(new Path(srcPath),new Path(desPath));
                return true;
            }else {
                throw new IOException("desPath is not exist");
            }
        } catch (IOException e) {
            return false;
        }finally {
            try {
                coreSys.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //文件下载
    public boolean downloadFile(String srcPath,String desPath){
        FileSystem coreSys=getCorSys();
        try {
            coreSys.copyToLocalFile(new Path(srcPath),new Path(desPath));
            return true;
        } catch (IOException e) {
            return false;
        }finally {
            try {
                coreSys.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //获取文件信息
    public IOVFile getFileInfo(String path) {
        FileSystem coreSys=getCorSys();
        try {
            return new IOVFile(coreSys.getFileStatus(new Path(path)));
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                coreSys.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}