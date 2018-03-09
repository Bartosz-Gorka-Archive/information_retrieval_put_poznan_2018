import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.langdetect.OptimaizeLangDetector;
import org.apache.tika.language.detect.LanguageResult;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.joda.time.DateTime;
import org.xml.sax.SAXException;

public class Exercise2 {
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private OptimaizeLangDetector langDetector;

    public static void main(String[] args) {
        Exercise2 exercise = new Exercise2();
        exercise.run();
    }

    private void run() {
        try {
            if (!new File("../outputDocuments").exists()) {
                Files.createDirectory(Paths.get("../outputDocuments"));
            }

            initLangDetector();

            File directory = new File("../documents");
            File[] files = directory.listFiles();
            for (File file : files) {
                processFile(file);
            }
        } catch (IOException | SAXException | TikaException e) {
            e.printStackTrace();
        }

    }

    private void initLangDetector() throws IOException {
        langDetector = new OptimaizeLangDetector();
        langDetector.loadModels();
    }

    private void processFile(File file) throws IOException, SAXException, TikaException {
        System.out.println(file.getName());
        InputStream inp = TikaInputStream.get(file);
        AutoDetectParser parser = new AutoDetectParser();
        BodyContentHandler handler = new BodyContentHandler(250000);
        Metadata metadata = new Metadata();

        parser.parse(inp, handler, metadata);
        String content = handler.toString();

        langDetector.reset();
        langDetector.addText(content);
        LanguageResult lang = langDetector.detect();

        Tika tika = new Tika();
        String mimeType = tika.detect(file);

        String creatorName = metadata.get(TikaCoreProperties.CREATOR);
        DateTime creationDate = new DateTime(metadata.get(TikaCoreProperties.CREATED));
        DateTime lastModification = new DateTime(metadata.get(TikaCoreProperties.MODIFIED));

        saveResult(
                file.getName(),
                lang.getLanguage(),
                creatorName,
                creationDate.toDate(),
                lastModification.toDate(),
                mimeType,
                content
        );
    }

    private void saveResult(String fileName, String language, String creatorName, Date creationDate,
                            Date lastModification, String mimeType, String content) {

        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
        int index = fileName.lastIndexOf(".");
        String outName = fileName.substring(0, index) + ".txt";
        try {
            PrintWriter printWriter = new PrintWriter("./outputDocuments/" + outName);
            printWriter.write("Name: " + fileName + "\n");
            printWriter.write("Language: " + (language != null ? language : "") + "\n");
            printWriter.write("Creator: " + (creatorName != null ? creatorName : "") + "\n");
            String creationDateStr = creationDate == null ? "" : dateFormat.format(creationDate);
            printWriter.write("Creation date: " + creationDateStr + "\n");
            String lastModificationStr = lastModification == null ? "" : dateFormat.format(lastModification);
            printWriter.write("Last modification: " + lastModificationStr + "\n");
            printWriter.write("MIME type: " + (mimeType != null ? mimeType : "") + "\n");
            printWriter.write("\n");
            printWriter.write(content + "\n");
            printWriter.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

}
