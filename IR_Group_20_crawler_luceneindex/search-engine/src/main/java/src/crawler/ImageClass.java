package src.crawler;// Image object which has attributes URl and Alt text (information about image)

public class ImageClass {
    String imageUrl;
    String text;

    public ImageClass(String URl , String alt) {
        imageUrl = URl;
        text = alt;
    }
}
