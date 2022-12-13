package src.indexer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Time;
import java.util.Date;


public class Indexer {
    public static void main(String[] args) {

        if(args.length != 3)
        {
            System.out.println(" Please enter 3 command line arguments in the order - inputPath OutputPath queryString");
            return;
        }

        // Reading data from command line arguments.
        String inputPathDirectory = args[0];
        String outputPath = args[1];
        String queryString = args[2];

        File curDirectory = new File(inputPathDirectory);

        try {
            Directory directory = FSDirectory.open(Paths.get(outputPath));

            Analyzer analyzer = new StandardAnalyzer();

            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

            long startTime = System.currentTimeMillis();

            IndexWriter writer = new IndexWriter(directory, indexWriterConfig);

            long endTime = System.currentTimeMillis();

            System.out.println("Time taken for Indexing: " + (endTime-startTime) + "milliseconds");

            System.out.println();

            indexDocs(writer, curDirectory);
            writer.close();

            //search
            DirectoryReader indexReader = DirectoryReader.open(directory);
            IndexSearcher indexSearcher = new IndexSearcher(indexReader);
            MultiFieldQueryParser multiFieldQueryParser = new MultiFieldQueryParser( new String[] {LuceneConstants.TITLE, LuceneConstants.PARA}, analyzer);

            Query query = multiFieldQueryParser.parse(queryString);

            int topHitCount = 100;
            TopDocs topDocs = indexSearcher.search(query, topHitCount);
            int rank = 0;

            for (final ScoreDoc scoreDoc : topDocs.scoreDocs) {

                System.out.println("Rank-[" + (rank++ + 1) +"]  " + scoreDoc);

                Document hitDoc = indexSearcher.doc(scoreDoc.doc);
                System.out.println("Document Path: " + hitDoc.get(LuceneConstants.FILE_LINK));

                String content = hitDoc.get(LuceneConstants.PARA);
                System.out.println("Page Content Preview: " + content.substring(0, content.indexOf('.') + 1));

                System.out.println("Query Result Explanation: ");
                System.out.println(indexSearcher.explain(query, scoreDoc.doc));
                System.out.println("------------------------------------------------------------------------------------------------------------------------------------------------------------------------------");
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

    }

    private static void indexDocs(IndexWriter writer, File curDirectory) throws IOException {

        // Fetch each file from the input directory.
            String[] arrFile = curDirectory.list();
            for(String curFile : arrFile)
            {
                String thisPath = curDirectory + "/" + curFile.trim();
                indexDocument(writer, thisPath);
            }
    }

    private static String readFile(String filePath) {

        String fileContent = "";

        try {
            fileContent = new String(Files.readAllBytes(Paths.get(filePath)));
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        return fileContent;
    }

    private static void indexDocument(IndexWriter writer, String inputFilePath) throws IOException {

        String fileText = readFile(inputFilePath);
        org.jsoup.nodes.Document htmlDoc = Jsoup.parse(fileText);

        Elements eleParas = htmlDoc.body().getElementsByTag("p");
        Elements eleAnchors = htmlDoc.select("a");

        StringBuilder strTitles = new StringBuilder();
        for(Element ele : eleAnchors)
        {
            strTitles.append(ele.attr(LuceneConstants.TITLE));
            strTitles.append(" ");
        }

        StringBuilder strParas = new StringBuilder();
        for(Element ele : eleParas)
        {
            strParas.append(ele.text());
            strParas.append(" ");
        }

        Document doc = new Document();
        doc.add(new TextField(LuceneConstants.FILE_LINK, inputFilePath, Field.Store.YES));
        doc.add(new TextField(LuceneConstants.PARA, strParas.toString(), Field.Store.YES));
        doc.add(new TextField(LuceneConstants.TITLE, strTitles.toString(), Field.Store.YES));

        writer.addDocument(doc);

    }
}
