import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;

import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;

import PI.PiJob;

public class Test {
	//Post Jar to Livy
public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ExecutionException {
	LivyClient client = new LivyClientBuilder()
			  .setURI(new URI("http://192.168.109.130:8998"))
			  .build();
			try {
			  System.err.printf("Uploading %s to the Spark context...\n", "D:/PI.jar");
			  client.uploadJar(new File("D:/PI.jar")).get();

			  System.err.printf("Running PiJob with %d samples...\n", 3);
			  Double pi = client.submit(new PiJob(3)).get();

			  System.out.println("Pi is roughly: " + pi);
			} finally {
			  client.stop(true);
			}
}
}
