import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.sax.PhoneExtractingContentHandler;
import org.apache.tika.sax.BodyContentHandler;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.apache.tika.parser.AutoDetectParser;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class Exercise1 {
    public static void main(String[] args) throws IOException, ParserConfigurationException, SAXException, TikaException {
        Exercise1 e1 = new Exercise1();
        e1.run();
    }

    private void run() throws ParserConfigurationException, SAXException, IOException, TikaException {
        LinkedList<String> phonesByTwoParses = exercise1a();
        System.out.println("Results of the two parses:");
        printResults(phonesByTwoParses);

        LinkedList<String> phonesByTika = exercise1b();
        System.out.println("Results of Tika:");
        printResults(phonesByTika);
    }


    private LinkedList<String> exercise1a() throws IOException, ParserConfigurationException, SAXException {
        System.out.println("Running exercise 1a...");
        LinkedList<String> results = new LinkedList<>();

        ZipFile file = new ZipFile("../Exercise1.zip");
        Enumeration entries = file.entries();
        while (entries.hasMoreElements()) {
            ZipEntry entry = (ZipEntry) entries.nextElement();
            InputStream stream = file.getInputStream(entry);

            // PDF file
            if (entry.getName().endsWith("pdf")) {
                System.out.println("PDF");
                PDDocument pdfFile = PDDocument.load(stream);
                PDFTextStripper pdfStripper = new PDFTextStripper();

                String content = pdfStripper.getText(pdfFile);

                Pattern pattern = Pattern.compile("\\([0-9]{3}\\) ?[0-9-]+");
                Matcher matcher = pattern.matcher(content);
                while (matcher.find()) {
                    String group = matcher.group();
                    group = group.replaceAll("[ ()-]", "");
                    results.add(group);
                }
            } else if (entry.getName().endsWith("xml")) {
                System.out.println("XML");

                DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                DocumentBuilder db = dbf.newDocumentBuilder();
                Document document = db.parse(stream);

                NodeList phonesList = document.getElementsByTagName("Phone");
                for (int i = 0; i < phonesList.getLength(); i++) {
                    String textContent = phonesList.item(i).getTextContent();
                    textContent = textContent.replaceAll("[ ()-]", "");
                    results.add(textContent);
                }
            } else {
                System.out.println("Not matched extension!");
            }
        }

        return results;
    }

    private LinkedList<String> exercise1b() throws IOException, TikaException, SAXException {
        System.out.println("Running exercise 1b...");
        LinkedList<String> results = new LinkedList<>();

        InputStream inp = TikaInputStream.get(new File("../Exercise1.zip"));
        AutoDetectParser parser = new AutoDetectParser();
        BodyContentHandler handler = new BodyContentHandler();
        Metadata metadata = new Metadata();
        PhoneExtractingContentHandler phoneHandler = new PhoneExtractingContentHandler(handler, metadata);

        parser.parse(inp, phoneHandler, metadata);
        String[] phoneNumbers = metadata.getValues("phonenumbers");

        List<String> strings = Arrays.asList(phoneNumbers);
        results.addAll(strings);

        return new LinkedList<>(results);
    }


    private void printResults(LinkedList<String> results) {
        LinkedList<String> parsedResults = new LinkedList<>();
        for (String s2 : results) {
            String s1 = s2.replace(" ", "");
            s1 = s1.replace("(", "");
            s1 = s1.replace(")", "");
            s1 = s1.replace("-", "");
            parsedResults.add(s1);
        }

        LinkedList<String> reference = new LinkedList<>(Arrays.asList(_data));
        LinkedList<String> incorrectly = new LinkedList<>();
        LinkedList<String> missed = new LinkedList<>();

        System.out.println("- detected phone numbers = " + parsedResults.size());
        int t = 0;
        for (String s1 : parsedResults)
            if (reference.contains(s1)) t++;
            else incorrectly.add(s1);

        for (String s1 : reference)
            if (!parsedResults.contains(s1))
                missed.add(s1);

        System.out.println("- correctly detected phone numbers = " + t);
        System.out.println("- incorrect detected phone numbers = " + incorrectly.size());
        for (String s : incorrectly)
            System.out.println("    " + s);
        System.out.println("- missed phone numbers = " + missed.size());
        for (String s : missed)
            System.out.println("    " + s);

    }

    private String _data[] = {
            "7256915622",
            "4289519018",
            "9402503286",
            "7785524197",
            "2041812298",
            "8142693192",
            "8726549759",
            "6615468662",
            "2048492437",
            "2922715420",
            "6427002319",
            "5415207134",
            "4114517460",
            "4196534921",
            "9154153005",
            "8666147732",
            "9237188291",
            "5603491440",
            "9571159173",
            "5482570883",
            "3509228486",
            "6478692640",
            "1235045237",
            "7683331038",
            "4451512278",
            "3168666968",
            "6683602279",
            "4155419711",
            "2355926980",
            "9278612234",
            "6732151992",
            "1297121453",
            "9945831692",
            "1074548772",
            "2742648914",
            "2116605343",
            "6948486721",
            "8692859252",
            "1126740391",
            "1765449317",
    };
}