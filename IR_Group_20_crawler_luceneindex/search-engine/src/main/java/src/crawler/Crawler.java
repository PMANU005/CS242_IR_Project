package src.crawler;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

public class Crawler {
    private String URL;
    // Queue of links
    Queue<String> links;
    // visited link
    HashMap<String , Integer> visitedLinks;
    WriteToFile w;
    public Crawler(String URL) {
        this.URL = URL;
        links = new LinkedList<>();
        visitedLinks = new HashMap<>();
    }
    
    // method to crawl the URL iteratively
    public void crawlUrl () {
        // for storing links
        System.out.println("crawler started");
        links.add(URL);
        int i = 0;   // counter remove it after checking

        try {
            while(!links.isEmpty() &&  i<6000) {
                System.out.println("parsing the page now");
                // https://stackoverflow.com/questions/24475816/jsoup-404-error - handling error
                // ignore http errors and allow only those with 200 status code
                Connection connection = Jsoup.connect(links.peek()).ignoreHttpErrors(true);
                Document doc =  connection.get();

                if(connection.response().statusCode() == 200) {   // need to check for pages with error codes
                    System.out.println("Peek Link: " + links.peek());
                    //  System.out.println(doc.outerHtml());
                    for(Element link : doc.getElementsByTag("a")) {
                        //  System.out.println("Link : " + link);
                        String innerLink = link.attr("abs:href");
                        String innerText = link.text();

                        // add to queue if link is not visited
                        //System.out.println("link now: " + innerLink);
                        if(!visitedLinks.containsKey(innerLink) && !innerLink.isEmpty()) {   // need to check some conditions
//                            if(!innerLink.contains("https") || !innerLink.contains("http")) {
//                                innerLink = hostLink.concat(innerLink);
//                                System.out.println("link now: " + innerLink);
//                            }
                            links.add(innerLink);
                            visitedLinks.put(innerLink , 1);    // need to stem this for checking duplicates
                        }else {
                            System.out.println("Visited link : " + innerLink + " skipping the link");
                        }
                    }
                    links.poll();
                    System.out.println("-------------------------------------");
                    i++;

                    // get data and write to file
                   // Elements textData = doc.getElementsByTag("");
                    //Elements textData = doc.select(".mw-content-ltr p, .mw-content-ltr li");   // for wikipedia
                   // Elements textData = doc.select("content");
                    // https://stackoverflow.com/questions/23910841/how-to-extract-text-from-wikipedia-using-jsoup
                    Elements textData = doc.getElementsByClass("webpage-content");
                    String title = doc.title();

                    // Element textData = doc.getElementById("webpage-content");
                    //  System.out.println("textData :"  + textData);
                    // object to write data to files
                    //WriteToFile w = new WriteToFile();
                    w.fileWrite(String.valueOf(textData), i);

                }else{
                    // skipping the link when response code is 404
                    System.out.println("Cannot parse Status 404");
                    links.poll();
                }
            }
        }catch (Exception e) {
            System.out.println("exception crawling: " + e);
        }
    }
}
