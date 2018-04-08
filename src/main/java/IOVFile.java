import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class IOVFile {
    public static String SEPARATOR = "/";
    private FileStatus fileStatus;
    private Path path;

    private IOVFile(String path) {
        this.path = new Path(path);
    }

    public IOVFile(FileStatus fileStatus) {
        this.fileStatus = fileStatus;
        this.path = fileStatus.getPath();
    }

    public FileStatus fileStatus() {
        return this.fileStatus;
    }

    public void setFileStatus(FileStatus fileStatus) {
        this.fileStatus = fileStatus;
    }

    public String getPath() {
        return this.path.toUri().getPath();
    }

    public String getName() {
        return this.path.getName();
    }

    public long getLength() {
        return this.fileStatus.getLen();
    }

    public String getParent() {
        return this.path.getParent().toUri().getPath();
    }

    public boolean isDirectory() {
        return this.fileStatus.isDirectory();
    }

    public long getModificationTime() {
        return this.fileStatus.getModificationTime();
    }
}