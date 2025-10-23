package com.edu.ensias.hadoop.hdfslab;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class WriteHDFS {
    public static void main(String[] args) throws IOException {

        // Vérification des arguments
        if (args.length < 2) {
            System.err.println("Usage: hadoop jar WriteHDFS.jar <chemin_du_fichier_HDFS> <contenu>");
            System.exit(1);
        }

        String hdfsFilePath = args[0]; // Exemple : /user/root/input/bonjour.txt
        String fileContent = args[1];  // Contenu à écrire

        // Configuration Hadoop
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path filePath = new Path(hdfsFilePath);

        // Vérifier si le fichier existe déjà
        if (fs.exists(filePath)) {
            System.out.println("Le fichier existe déjà : " + hdfsFilePath);
        } else {
            // Création et écriture du fichier
            try (FSDataOutputStream outStream = fs.create(filePath)) {
                outStream.writeUTF("Bonjour tout le monde !\n");
                outStream.writeUTF(fileContent + "\n");
                System.out.println("Fichier créé avec succès : " + hdfsFilePath);
            }
        }

        fs.close();
    }
}
