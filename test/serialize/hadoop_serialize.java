
/**
 * Simple program to write a few numbers into a binary stream.
 *
 * The resulting binary file is used to test the Python deserialization
 * functions.
 *
 */
import org.apache.hadoop.io.WritableUtils;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;


public class hadoop_serialize {

	public static void main(String[] args) throws java.io.IOException {
		DataOutputStream os = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("java_ostream.bin")));

		System.out.println("Writing a sequence of numbers");

		System.out.println("WritableUtils.writeVInt: 42, 4242, 424242, 42424242, -42");
		WritableUtils.writeVInt(os, 42);
		WritableUtils.writeVInt(os, 4242);
		WritableUtils.writeVInt(os, 424242);
		WritableUtils.writeVInt(os, 42424242);
		WritableUtils.writeVInt(os, -42);

		System.out.println("WritableUtils.writeVLong 42, 424242, 4242424242");
		WritableUtils.writeVLong(os, 42L);
		WritableUtils.writeVLong(os, 424242L);
		WritableUtils.writeVLong(os, 4242424242L);

		System.out.println("WritableUtils.writeString \"hello world\"");
		WritableUtils.writeString(os, "hello world");
		WritableUtils.writeString(os, "This file contains: writeVInt of 42, 4242, 424242, 42424242, -42; writeVLong of 42, 424242, 4242424242; 2 writeString calls");


		os.close();
	}
}
