/* BEGIN_COPYRIGHT
 *
 * Copyright 2009-2018 CRS4.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * END_COPYRIGHT
 */

/**
 * Simple program to write a few numbers into a binary stream.
 *
 * The resulting binary file is used to test the Python deserialization
 * functions.
 *
 */

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;


public class hadoop_serialize {

	public static void main(String[] args) throws java.io.IOException {
		//System.err.println("Writing byte stream to stdout");
		DataOutputStream os = new DataOutputStream(System.out);

		//System.err.println("Writing a sequence of numbers");

		//System.err.println("WritableUtils.writeVInt: 42, 4242, 424242, 42424242, -42");
		WritableUtils.writeVInt(os, 42);
		WritableUtils.writeVInt(os, 4242);
		WritableUtils.writeVInt(os, 424242);
		WritableUtils.writeVInt(os, 42424242);
		WritableUtils.writeVInt(os, -42);

		//System.err.println("WritableUtils.writeVLong 42, 424242, 4242424242");
		WritableUtils.writeVLong(os, 42L);
		WritableUtils.writeVLong(os, 424242L);
		WritableUtils.writeVLong(os, 4242424242L);
        //
		//System.err.println("WritableUtils.writeString \"hello world\"");
		WritableUtils.writeString(os, "hello world");
		WritableUtils.writeString(os, "oggi \u00e8 gioved\u00ec");
        
		// This file contains: writeVInt of 42, 4242, 424242, 42424242, -42; writeVLong of 42, 424242, 4242424242; 2 writeString calls

		//System.err.println("Text.write \"I'm a Text object\"");
		Text t = new Text("\u00e0 Text object");
		t.write(os);

		os.close();
	}
}
