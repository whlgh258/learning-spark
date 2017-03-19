import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

/**
 * Created by wanghl on 17-3-19.
 */
public class ScannerTest {

    public static void main(String[] args) throws FileNotFoundException {
        Scanner scanner = new Scanner(new File("README.md")).useDelimiter("\n");
        while(scanner.hasNext()){
            System.out.println(scanner.next());
        }
    }
}
