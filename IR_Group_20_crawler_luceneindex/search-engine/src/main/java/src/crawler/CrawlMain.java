package src.crawler;

import java.io.File;
import java.util.ArrayList;

public class CrawlMain {
    public static void main (String args[]) throws InterruptedException {

        System.out.println("Enter the command line arguments 1 -> URL 2-> Output Directory 3 -> Thread count");
        long startTime = System.currentTimeMillis();
        String URL = args[0];
        String dirPath = args[1];
        int depth = Integer.parseInt(args[2]);
        int no_of_threads = Integer.valueOf(args[3]);
        // check if directory exists so if exists clean the directory
        File file = new File(dirPath);
        if(file.exists() && file.isDirectory()) {
            for (File f : file.listFiles()) {
                f.delete();
            }
        }else{
            System.out.println("Give appropriate output directory");
        }

        MultiCrawler parentCrawler = new MultiCrawler(URL , args[1] , depth); // parent thread

        // child threads
        ArrayList<MultiCrawler> child_crawler = new ArrayList<>(no_of_threads);
        for(int thread_counter=0; thread_counter<no_of_threads;thread_counter++){
            child_crawler.add(new MultiCrawler(dirPath));
        }
        // makes main thread execution until the crawler threads complete
        Thread.sleep(5000);
        ArrayList<ImageClass> arr = parentCrawler.getImageList();
        parentCrawler.getThread().join();
        Thread.sleep(20000);
        for(int thread_counter=0; thread_counter<no_of_threads;thread_counter++){
            child_crawler.get(thread_counter).getThread().join();
        }
        System.out.println("Completed");
        long endTime = System.currentTimeMillis();
        System.out.println(endTime - startTime);


/*
        String dirPath = "/Users/avinash/Documents/Retrieval/IR/";
        File file = new File(dirPath+"Data");
        if(file.exists() && file.isDirectory()) {
            for (File f :file.listFiles()) {
                f.delete();
            }
        }else{
            System.out.println("Give appropriate output directory");
        }

        long startTime = System.currentTimeMillis();

        //String URL = "https://www.ucr.edu/";

        //String URL = "https://en.wikipedia.org/wiki/California";
        //String URL = "https://en.wikipedia.org/wiki/United_States";
        //String URL = "https://en.wikipedia.org/wiki/India";
        //String URL =  "https://en.wikipedia.org/wiki/Rachel_Green#The_";
        String URL = "https://en.wikipedia.org/wiki/Friends";
       // String URL = "https://en.wikipedia.org/wiki/Friends#mw-head";
        //String URL = "https://www.americaslibrary.gov/es/ca/es_ca_subj.html";

        MultiCrawler cmain = new MultiCrawler(URL , dirPath , 500 );    // main thread
        cmain.getThread().setName("parent");
        MultiCrawler cmain1 = new MultiCrawler(dirPath);
        MultiCrawler cmain2 = new MultiCrawler(dirPath);
        MultiCrawler cmain3 = new MultiCrawler(dirPath);
        MultiCrawler cmain4 = new MultiCrawler(dirPath);
//        MultiCrawler cmain5 = new MultiCrawler(dirPath);
//        MultiCrawler cmain6 = new MultiCrawler(dirPath);
//        MultiCrawler cmain7 = new MultiCrawler(dirPath);
//        MultiCrawler cmain8 = new MultiCrawler(dirPath);
//        MultiCrawler cmain5 = new MultiCrawler(dirPath);
//        MultiCrawler cmain6 = new MultiCrawler(dirPath);
//        MultiCrawler cmain7 = new MultiCrawler(dirPath);
//        MultiCrawler cmain8 = new MultiCrawler(dirPath);
//        MultiCrawler cmain9 = new MultiCrawler(dirPath);
//        MultiCrawler cmain10 = new MultiCrawler(dirPath);
//        MultiCrawler cmain11 = new MultiCrawler(dirPath);
//        MultiCrawler cmain12 = new MultiCrawler(dirPath);
//        MultiCrawler cmain13 = new MultiCrawler(dirPath);
//        MultiCrawler cmain14 = new MultiCrawler(dirPath);
        System.out.println("Waiting");
        Thread.sleep(5000);
        ArrayList<ImageClass> arr = cmain.getImageList();
        cmain.getThread().join();
        cmain1.getThread().join();
        cmain2.getThread().join();
        cmain3.getThread().join();
        cmain4.getThread().join();
//        cmain5.getThread().join();
//        cmain6.getThread().join();
//        cmain7.getThread().join();
//        cmain8.getThread().join();
//        cmain5.getThread().join();
//        cmain6.getThread().join();
//        cmain7.getThread().join();
//        cmain8.getThread().join();
//        cmain9.getThread().join();
//        cmain10.getThread().join();
//        cmain11.getThread().join();
//        cmain12.getThread().join();
//        cmain13.getThread().join();
//        cmain14.getThread().join();
        System.out.println("Completed");
        long endTime = System.currentTimeMillis();
*/
//        System.out.println(endTime - startTime);
        WriteToFile wf = new WriteToFile(dirPath);
        for(ImageClass img : arr){
            wf.singleImageStoreWrite(img);
        }

    }
}
