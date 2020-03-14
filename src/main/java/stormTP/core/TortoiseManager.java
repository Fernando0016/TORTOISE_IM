package stormTP.core;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;

public class TortoiseManager {

    public static final String PROG = "C'est mieux!";
    public static final String CONST = "Statu quo";
    public static final String REGR = "ça sera sûrement mieux plus tard!";

    String nomsBinome = "";
    long dossard = -1;

    public TortoiseManager(long dossard, String nomsBinome) {
        this.nomsBinome = nomsBinome;
        this.dossard = dossard;
    }
    
    public TortoiseManager(){
        this.nomsBinome = "KERAGHEL-TOUATIOUI";
        this.dossard = 3;
    }
    /**
     * Permet de filtrer les informations concernant votre tortue
     *
     * @param input
     * @return
     */
    public Runner filter(String input) {

        Runner tortoiseRunner = null;
        JsonReader jsonReader = Json.createReader(new StringReader(input));
        JsonObject jsonObj = jsonReader.readObject();
        jsonReader.close();

        JsonArray tortoises = jsonObj.getJsonArray("tortoises");
        JsonObject tortoise = null;

        for (int i = 0; i < tortoises.size(); i++) {
            if (tortoises.getJsonObject(i).getInt("id") == this.dossard) {
                tortoise = tortoises.getJsonObject(i);
            }
        }

        long id = tortoise.getInt("id");
        int before = tortoise.getInt("nbDevant");
        int after = tortoise.getInt("nbDerriere");
        int total = tortoise.getInt("total");
        int position = tortoise.getInt("position");
        long top = tortoise.getInt("top");
        tortoiseRunner = new Runner(id, this.nomsBinome, before, after, total, position, top);

        return tortoiseRunner;
    }

    public Runner computeRank(long id, long top, String nom, int nbDevant, int nbDerriere, int nbTotal) {
        Runner tortoiseRunner = new Runner(id, nom, nbDevant, nbDerriere, nbTotal, 0, top);
        if ((nbDevant + nbDerriere) + 1 == nbTotal) {
            tortoiseRunner.setRang(String.valueOf(nbDevant + 1));
        } else {
            tortoiseRunner.setRang((nbDevant + 1) + "ex");
        }
        return tortoiseRunner;
    }

    public String getNomsBinome() {
        return nomsBinome;
    }

    public long getDossard() {
        return dossard;
    }

    public Integer computePoints(String rang, int nbTotal) {
        int monRang;
        if (rang.contains("ex")) {
            monRang = Integer.parseInt(rang.substring(0, rang.length() - 2));
        } else {
            monRang = Integer.parseInt(rang);
        }
        if (monRang == 1) {
            return 12;
        } else {
            if (monRang == 2) {
                return 10;
            } else {
                if (monRang == 3) {
                    return 8;
                } else {
                    return nbTotal - monRang;
                }
            }

        }
    }
    
    public String giveRankEvolution(double rank1, double rank2) {
        if (rank1 == rank2)
            return TortoiseManager.CONST;
        else if (rank1 < rank2)
            return TortoiseManager.PROG;
        else
            return TortoiseManager.REGR;
    }

}
