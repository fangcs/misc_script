import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.PriorityQueue;

public class Twitter2GPS implements Runnable{
	
	public static String originalFileName = null;
	public static final String errorFileName = "error.log";
	public static final String tempDirName = "temp";
	public static int splitNum = 4;
	public static int currentIndex = 0;
	public static int workNum = 1;
	public static final int BUFFER_SIZE = 4194304;
	public static byte[] lock = new byte[0];
	
	public static int getNextIndex(){
		synchronized(lock){
			if (currentIndex < splitNum){
				currentIndex ++;
				System.out.println(String.format("Worker-%d is processing %d file.", Thread.currentThread().getId(), currentIndex - 1));
				return currentIndex - 1 ;
			}else{
				System.out.println(String.format("Worker-%d is off.", Thread.currentThread().getId()));
				return -1;
			}
		}
	}
	
	public static void splitFile() {
		if (splitNum < 1) {
			splitNum = 10;
		}
		long lineNum = 0;
		File file = new File(originalFileName);
		try {
			BufferedReader br = new BufferedReader(new FileReader(file), BUFFER_SIZE);
			String line = null;
			BufferedWriter[] tempBRs = new BufferedWriter[splitNum];
			File tempDir = new File(tempDirName);
			if (!tempDir.exists()) {
				tempDir.mkdir();
			}
			for (int i = 0; i < splitNum; ++i) {
				tempBRs[i] = new BufferedWriter(new FileWriter(String.format(
						"%s/%s_temp_%d", tempDirName, file, i)));
			}
			line = br.readLine();
			++ lineNum;
			while (line != null) {
				String[] tempArray = line.split("\\s+");
				if (tempArray.length > 1) {
					long userID = Long.parseLong(tempArray[0]);
					long follower = Long.parseLong(tempArray[1]);
					tempBRs[(int)(follower % splitNum)].write(String.format(
							"%d\t%d\n", follower, userID));
					tempBRs[(int) (userID % splitNum)].write(String.format("%d\n",
							userID));
				} else {
					System.err.println(String.format("Error data(during inverting)! %s\n", line));
				}
				line = br.readLine();
				++ lineNum;
				if (lineNum % 1000000 == 0){
					System.out.println(String.format("Processed %d lines", lineNum));
				}
			}
			for (int i = 0; i < splitNum; ++i) {
				tempBRs[i].close();
			}
			br.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void sortSplitedFile(int index) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(
					String.format("%s/%s_temp_%d", tempDirName, originalFileName, index)), BUFFER_SIZE);
			String line = br.readLine();
			ArrayList<Edge> edges = new ArrayList<Edge>();
			HashSet<Long> srcSet = new HashSet<Long>();
			while (line != null) {
				String[] tempArray = line.split("\\s+");
				if (tempArray.length > 1) {
					long follower = Long.parseLong(tempArray[0]);
					long user = Long.parseLong(tempArray[1]);
					Edge thisEdge = new Edge(follower, user);
//					int index = Collections.binarySearch(edges, thisEdge);
//					if (index < 0){
//						edges.add(0, thisEdge);
//					}else if (index >= edges.size()){
//						edges.add(thisEdge);
//					}else{
//						edges.add(index, thisEdge);
//					}
					edges.add(thisEdge);
					srcSet.add(follower);
				} else if (tempArray.length == 1) {
					long follower = Long.parseLong(tempArray[0]);
					long user = -1;
					if (!srcSet.contains(follower)){
						Edge thisEdge = new Edge(follower, user);
//					int index = Collections.binarySearch(edges, thisEdge);
//					if (index < 0){
//						edges.add(0, thisEdge);
//					}else if (index >= edges.size()){
//						edges.add(thisEdge);
//					}else{
//						edges.add(index, thisEdge);
//					}
						edges.add(thisEdge);
						srcSet.add(follower);
					}
				} else {
					System.err.println(String.format("Error data (during sorting)! %s\n", line));
				}
				line = br.readLine();
			}
			br.close();
			br = null;
			Collections.sort(edges);
			BufferedWriter bw = new BufferedWriter(new FileWriter(
					String.format("%s/%s_temp_sorted_%d", tempDirName, originalFileName, index)));
			int edgeNum = edges.size();
			for (int i=0; i<edgeNum; ++i){
				Edge thisEdge = edges.get(i);
				bw.write(String.format("%d\t%d\n", thisEdge.src, thisEdge.dst));
			}
			edges.clear();
			edges = null;
			srcSet.clear();
			srcSet = null;
			bw.close();
			bw = null;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static SrcIndex parseFirstLine(BufferedReader br, int index){
		
		try {
			String line = br.readLine();
			if (line != null){
				String []tempArray = line.split("\\s+");
				long follower = Long.parseLong(tempArray[0]);
				long user = Long.parseLong(tempArray[1]);
				SrcIndex result = new SrcIndex(follower, index);
				if (user > -1){
					result.outList.add(user);
				}
				return result;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public static SrcIndex parseLinesUntilNextEntity(BufferedReader br, SrcIndex currentEntity){
		
		try {
			String line = br.readLine();
			long follower = -1;
			long user = -1;
			SrcIndex result = null;
			while (line != null){
				String []tempArray = line.split("\\s+");
				follower = Long.parseLong(tempArray[0]);
				user = Long.parseLong(tempArray[1]);
				if (follower == currentEntity.src){
					if (user > -1){
						currentEntity.outList.add(user);
					}
				}else{
					result = new SrcIndex(follower, currentEntity.index);
					if (user > -1){
						result.outList.add(user);
					}
					return result;
				}
				line = br.readLine();
			}
			return result;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public static void merge(){
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(originalFileName + "_GPS")));
			BufferedReader[] br = new BufferedReader[splitNum];
			for (int i=0; i<splitNum; ++i){
				br[i] = new BufferedReader(new FileReader(
						String.format("%s/%s_temp_sorted_%d", tempDirName, originalFileName, i)));
				System.out.println("open " + String.format("%s/%s_temp_sorted_%d", tempDirName, originalFileName, i));
			}
			
			PriorityQueue<SrcIndex> pq = new PriorityQueue<SrcIndex>();
			for (int i=0; i<splitNum; ++i){
				pq.add(parseFirstLine(br[i], i));
			}
			System.out.println("pqsize: " + pq.size());
			int currentProcess = 0;
			while (!pq.isEmpty()){
				SrcIndex thisEntity = pq.poll();
				BufferedReader thisBR = br[thisEntity.index];
				SrcIndex nextEntity = parseLinesUntilNextEntity(thisBR, thisEntity);
				StringBuffer sb = new StringBuffer();
				sb.append(thisEntity.src);
				ArrayList<Long> outList = thisEntity.outList;
				int size = outList.size();
				for (int i=0; i<size; ++i){
					sb.append(" ");
					sb.append(outList.get(i));
				}
				bw.write(sb.toString());
				bw.write("\n");
				if (nextEntity != null){
					pq.add(nextEntity);
				}
				if (thisEntity.src > currentProcess){
					System.out.println("Output progress: " + thisEntity.src);
					currentProcess += 1000000;
				}
				
			}
			
			for (int i=0; i<splitNum; ++i){
				br[i].close();
			}
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public static void main(String args[]) {
		try {
			PrintStream errPS = new PrintStream(new FileOutputStream(new File(errorFileName)));
			System.setErr(errPS);
			
			System.out.println("Part 1: invert the positions of user and followers!");
			long startStamp = System.currentTimeMillis();
			
			
			originalFileName = args[0];
			//splitFile();
			long part1Stamp = System.currentTimeMillis();
			System.out.println(String.format("Part1 costs %d seconds.", (part1Stamp - startStamp) / 1000));
			
			System.out.println("Part 2: sort splited files! (Multithreads)");
//			Thread [] workers = new Thread[workNum];
//			for (int i=0; i<workers.length; ++i){
//				workers[i] = new Thread(new Twitter2GPS());
//			}
//			for (int i=0; i<workers.length; ++i){
//				workers[i].start();
//			}
//			for (int i=0; i<workers.length; ++i){
//				try {
//					workers[i].join();
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
			long part2Stamp = System.currentTimeMillis();
			System.out.println(String.format("Part2 costs %d seconds.", (part2Stamp - part1Stamp) / 1000));
			
			System.out.println("Part 3: sort splited files! (Multithreads)");
			merge();
			
			long endStamp = System.currentTimeMillis();
			System.out.println(String.format("Part3 costs %d seconds.", (endStamp - part2Stamp) / 1000));
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@Override
	public void run() {
		int index = -1;
		while ((index=getNextIndex()) > -1){
			
			sortSplitedFile(index);
//			try {
//				Thread.sleep(100000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
		}
	}
}

class SrcIndex implements Comparable<SrcIndex>{
	long src;
	int index;
	ArrayList<Long> outList = new ArrayList<Long>();
	
	public SrcIndex(long src, int index){
		this.src = src;
		this.index = index;
	}
	
	public int compareTo(SrcIndex s){
		if (src < s.src){
			return -1;
		}else if (src == s.src){
			return 0;
		}
		return 1;
	}
}

class Edge implements Comparable<Edge> {
	long src;
	long dst;

	Edge(long src, long dst) {
		this.src = src;
		this.dst = dst;
	}

	@Override
	public int compareTo(Edge o) {
		if (src < o.src) {
			return -1;
		} else if (src == o.src) {
			if (dst < o.dst) {
				return -1;
			} else if (dst == o.dst) {
				return 0;
			} else {
				return 1;
			}
		} else {
			return 1;
		}
	}
}
