package com.edu.ensias.hadoop.hdfslab;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus1 {
    public static void main(String[] args) {
        // Vérification des paramètres
        if (args.length != 3) {
            System.out.println("Usage: hadoop jar HadoopFileStatus.jar <chemin_fichier> <nom_fichier> <nouveau_nom_fichier>");
            System.exit(1);
        }

        String directoryPath = args[0];
        String fileName = args[1];
        String newFileName = args[2];

        Configuration conf = new Configuration();
        FileSystem fs = null;

        try {
            fs = FileSystem.get(conf);

            // Chemins HDFS
            Path filepath = new Path(directoryPath + "/" + fileName);
            Path newPath = new Path(directoryPath + "/" + newFileName);

            // Vérifier si le fichier existe
            if (!fs.exists(filepath)) {
                System.out.println("File does not exist: " + filepath);
                System.exit(1);
            }

            // Récupérer les informations du fichier
            FileStatus status = fs.getFileStatus(filepath);

            System.out.println("=== File Information ===");
            System.out.println("File Name: " + filepath.getName());
            System.out.println("File Path: " + filepath.toString());
            System.out.println("File Size: " + status.getLen() + " bytes");
            System.out.println("File Owner: " + status.getOwner());
            System.out.println("File Permission: " + status.getPermission());
            System.out.println("File Replication: " + status.getReplication());
            System.out.println("File Block Size: " + status.getBlockSize());

            // Récupérer les informations des blocs
            BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, status.getLen());
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                System.out.println("\nBlock offset: " + blockLocation.getOffset());
                System.out.println("Block length: " + blockLocation.getLength());
                System.out.print("Block hosts: ");
                for (String host : hosts) {
                    System.out.print(host + " ");
                }
                System.out.println();
            }

            // Renommer le fichier
            if (fs.rename(filepath, newPath)) {
                System.out.println("\nFile renamed successfully to: " + newFileName);
            } else {
                System.out.println("\nFailed to rename file.");
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (fs != null) fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
