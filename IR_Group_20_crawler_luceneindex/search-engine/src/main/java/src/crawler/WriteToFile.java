package src.crawler;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;

public class WriteToFile {
    String dirPath;
    static Integer count=1;
    public WriteToFile(String path) {
        dirPath = path;
    }

    public void fileWrite(String parsedData ,Integer counter){
      //  File file = "/Users/avinash/Documents/Retrieval/Data/";
        String fileName = dirPath + "Data/document_" + counter +".txt";
        try {
          //  String fileName = "/Users/avinash/Documents/Retrieval/Data/document_" + counter +".txt";
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(fileName));
            bufferedWriter.write(parsedData);
            bufferedWriter.close();
        }catch (Exception e) {
            System.out.println("Error Writing to a file");
        }
    }

    public void imageStoreWrite(ArrayList<ImageClass> imageStore) {
        for(ImageClass imgObj : imageStore) {
            try{
               // System.out.println("imgobj: " + imgObj.imageUrl);
                String fileName = dirPath +"ImageFolder/"+ count + imgObj.imageUrl.substring(imgObj.imageUrl.length()-4);
                InputStream imageReader = new BufferedInputStream(new URL(imgObj.imageUrl).openStream());
                OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(fileName));
                int readbyte;
                while((readbyte = imageReader.read())!=-1) {
                    outputStream.write(readbyte);
                }
                count++;
            }catch (Exception e){
                System.out.println("Error getting Image from URL");
            }
        }
    }

    public void singleImageStoreWrite(ImageClass imgObj) {
        try{
            String fileName = dirPath +"ImageFolder/"+ imgObj.text + imgObj.imageUrl.substring(imgObj.imageUrl.length()-4);
            InputStream imageReader = new BufferedInputStream(new URL(imgObj.imageUrl).openStream());
            OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(fileName));
            int readbyte;
            while((readbyte = imageReader.read())!=-1) {
                outputStream.write(readbyte);
            }
        }catch (Exception e){
            System.out.println("Error getting Image from URL");
        }
    }
}
