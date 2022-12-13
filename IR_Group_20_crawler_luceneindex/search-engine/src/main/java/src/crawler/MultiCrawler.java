package src.crawler;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

// multi threaded crawler implementation
public class MultiCrawler implements Runnable {

    private static String URL;
    // Queue of links
    static Queue<String> links;
    // visited link
    static HashMap<String , Integer> visitedLinks;
    static ArrayList<ImageClass> imageStore;   // list to store image object
    static Integer counter;
    static Integer MAX_Depth;
    WriteToFile w;
    Thread mainThread;

    // main constructor for parent thread
    public MultiCrawler(String URL , String dirPath, Integer Depth) {
        this.URL = URL;
        links = new LinkedList<>();
        visitedLinks = new HashMap<>();
        counter = 0;
        MAX_Depth = Depth;
        links.add(URL);
        w = new WriteToFile(dirPath);
        imageStore = new ArrayList<>();
        mainThread = new Thread(this);
        mainThread.start();
    }

    // overloaded constructor for child threads
    public MultiCrawler(String dirPath) throws InterruptedException {
        w = new WriteToFile(dirPath);    //
        mainThread = new Thread(this);
        mainThread.sleep(5000);
        mainThread.start();
    }

    @Override
    public void run() {
        System.out.println("Crawler Started");
        startCrawl();
        //w.imageStoreWrite(imageStore);   // after running the crawler download images
    }

    public void startCrawl() {
        System.out.println("------------- Thread " + this.getThread().getName() + " started crawling ----------------");
        try {
            while(!links.isEmpty() &&  counter < MAX_Depth) {
                // https://stackoverflow.com/questions/24475816/jsoup-404-error - handling error
                String peekLink = getPeekLink();   // synchronised method
                System.out.println("thread :" + getThread().getName() + " -  link: " + peekLink + " counter: " + counter);
                Document doc = getDocument(peekLink);
                if(doc != null && isUrlValid(peekLink)) {
                    for(Element link : doc.getElementsByTag("a")) {
                        //String innerLink = link.attr("abs:href").trim();
                        String innerLink = formatURL(link.absUrl("href").trim());
                        String iLink = link.attr("href");

                        if(!visitedLinks.containsKey(innerLink) && !innerLink.isEmpty() && !peekLink.contains(innerLink) && !iLink.contains("#")) {   // need to check some conditions
                            links.add(innerLink);
                            visitedLinks.put(innerLink , 1);    // need to stem this for checking duplicates
                        }else {
                           // System.out.println("Visited link : " + innerLink + " skipping the link");
                        }
                    }
                      // for image crawling
                    for(Element image : doc.select("img[src]")) {
                        String innerImage = image.absUrl("src");
                        String altText = String.valueOf(image.attr("alt"));
                        if(!visitedLinks.containsKey(innerImage) && !innerImage.isEmpty() && !altText.isEmpty()) {
                            imageStore.add(new ImageClass(innerImage , altText));
                        }
                    }
                    System.out.println("-------------------------------------");
                    // synchronised method increment the counter
                    //Elements textData = doc.select(".mw-content-ltr p, .mw-content-ltr li");
                    // Element textData = doc.getElementById("webpage-content");
                    Elements textData = doc.getElementsByTag("body");
                    Elements title = doc.select("title");
                    incrementCounter();
                    w.fileWrite(String.valueOf(textData),  counter); // object to write data to files
                }else{
                    // skipping the link when response code is other than 200
                    System.out.println("Cannot parse link : Response Status code is not 200");
                }
            }
        }catch (Exception e) {
            System.out.println("exception crawling: " + e);
        }
    }

    public String formatURL(String rawLink) {
        if(rawLink.contains("#")) {    // formatting the link to take care of duplicates
            rawLink = rawLink.substring(0,rawLink.indexOf('#')).trim();
        }else if(rawLink.contains("#")) {
            rawLink = rawLink.substring(0,rawLink.indexOf('?')).trim();
        }
        return rawLink;
    }

    public ArrayList getImageList() {
        return imageStore;
    }

    // return the document object
    public Document getDocument(String link) {
        try{
            Connection connection = Jsoup.connect(link).ignoreHttpErrors(true).followRedirects(false); // setting timeout to 10s
            Document doc =  connection.get();
            // ignore http errors and allow only those with 200 status code
            if(connection.response().statusCode() == 200) {
                return doc;
            }
            return null;
        }catch (Exception e) {
            return null;
        }
    }

    // returns peek link on queue
    public synchronized String getPeekLink() {
        while(!links.isEmpty() && links.peek() == null) {
            links.poll();
        }
        String peekLink = links.peek();
        links.poll();
        return peekLink;
    }

    // increment counter synchronously
    public synchronized void incrementCounter() {
        counter = counter+1;
    }

    // return this thread for join
    public Thread getThread() {
        return mainThread;
    }

    // checking malformed exception
    public static boolean isUrlValid(String url) {
        try {
            java.net.URL obj = new URL(url);
            obj.toURI();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

}
