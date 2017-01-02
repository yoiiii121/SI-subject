import java.util.Scanner;

import si.si.e1.TestWorldCount;
import si.si.e10.TestKmeans;
import si.si.e2.TestManagementDB_SQL;
import si.si.e3.TestManagementDB_Join_JSON;
import si.si.e4.TestManagementTwitter;
import si.si.e6.TestSparkSQLMLlibRSALS;
import si.si.e7.TestSparkSQLMLlibRSContentBased;
import si.si.e8.TestFPGrouwth;
import si.si.e9.TestFPGrouwth2;

public final class Run_Main {
	private static final int nexercises = 10;

	public static void main(String[] args) throws Exception {

		String sSistemaOperativo = System.getProperty("os.name");

		if (sSistemaOperativo.contains("Windows")) {

			System.setProperty("hadoop.home.dir", "c:\\winutil\\");

			System.setProperty("spark.sql.warehouse.dir",

					"file:///${System.getProperty(\"user.dir\")}/spark-warehouse".replaceAll("\\\\", "/"));

		}
		System.out.println("Program is starting");
		int opt;
		String entradaTeclado = "";
		Scanner entradaEscaner = new Scanner(System.in);
		do {
			System.out.println("Please: input a number from 1 to " + nexercises);
			System.out.println("Options:" + " \n1 = Unsorted words Count" + " \n2 = Extraction of one CSV column"
					+ " \n3 = Join of two JSON related tables" + " \n4 = Twitts extraction of Twitter"
					+ " \n5 = Twitts sentiment extraction of Twitter"
					+ " \n6 = Recomendation system hibrid using ALS algorithm"
					+ " \n7 = Recomendation system hibrid adding a similarity decimal number"
					+ " \n8 = Find Frequent Item Sets with the FP-growth Algorithm"
					+ " \n9 = Assign association rules of frequent Item Sets with the FP-growth Algorithm"
					+ " \n10 = Compute Clastering with Kmeans algorithm" + "\n");
			entradaTeclado = entradaEscaner.nextLine();

			try {
				opt = Integer.valueOf(entradaTeclado);
			} catch (NumberFormatException ne) {
				opt = nexercises + 1;
			}

			String[] path = new String[2];

			String computerUser = "Nacho";
			switch (opt) {
			case 1:
				path[0] = "./src/main/java/si/si/e1/";
				TestWorldCount.main(path);
				break;
			case 2:
				path[0] = "./src/main/java/si/si/e2/";
				TestManagementDB_SQL.main(path);
				break;
			case 3:
				path[0] = "./src/main/java/si/si/e3/";
				TestManagementDB_Join_JSON.main(path);
				break;
			case 4:
				System.out.println("if the execution have failed, you should change the value computerUser");
				path[0] = "/home/" + computerUser + "/Escritorio/Twitts/";
				path[1] = "C:\\Users\\" + computerUser + "\\Desktop\\Twitts\\";
				TestManagementTwitter.main(path);
				break;
			case 5:
				System.out.println("if the execution have failed, you should change the value computerUser");
				path[0] = "/home/" + computerUser + "/Escritorio/Twitts/";
				path[1] = "C:\\Users\\" + computerUser + "\\Desktop\\Twitts\\";
				TestManagementTwitter.main(path);
				break;
			case 6:
				TestSparkSQLMLlibRSContentBased.main(path);
				break;
			case 7:
				TestSparkSQLMLlibRSALS.main(path);
				break;
			case 8:
				TestFPGrouwth.main(path);
				break;
			case 9:
				TestFPGrouwth2.main(path);
				break;
			case 10:
				TestKmeans.main(path);
				break;
			default:
				System.out.println("Number invalid input the number again");
			}

		} while (opt < 1 || opt > nexercises);
		entradaEscaner.close();
	}

}