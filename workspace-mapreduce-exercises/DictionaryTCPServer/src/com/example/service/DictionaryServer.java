package com.example.service;

import java.io.File;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.util.List;

/**
 * 
 * @author Dr. Binnur Kurt <binnur.kurt@gmail.com>
 *
 */
public class DictionaryServer {

	private static ServerSocket serverSocket;
	private static final int port = 7070;

	public static void main(String[] args) throws Exception {
		serverSocket = new ServerSocket(port);
		File dictionary = new File("src", "dictionary.txt");
		List<String> words = Files.readAllLines(dictionary.toPath());

		System.err.println("Server is running at port " + port);

		while (true) {
			Socket socket = serverSocket.accept();
			OutputStream outputStream = socket.getOutputStream();
			for (int i = 0; i < 1000; ++i) {
				outputStream.write((words.get(i) + " ").getBytes());
			}
			socket.close();
		}

	}

}