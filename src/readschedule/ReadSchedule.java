package readschedule;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.zip.GZIPInputStream;
import javax.net.ssl.HttpsURLConnection;
import javax.xml.bind.DatatypeConverter;
import org.json.JSONArray;
import org.json.JSONObject;

public class ReadSchedule
{
    private static JSONObject AUTH;

    public static void main(String[] args)
    {
        long start = System.currentTimeMillis();

        boolean doSmartCorpus = false;
        boolean fullCif = true;
        boolean noSched = false;
        boolean isAuto = false;
        String cifDay = "";

        if (args.length == 0)
        {
            System.err.println("Usage ReadSchedule [doSmartCorpus] [isAuto] [noSched|dow] [fullCif]");
        }

        for (String arg : args)
        {
            if (arg.equalsIgnoreCase("doSmartCorpus"))
                doSmartCorpus = true;
            else if (arg.equals("isAuto"))
                isAuto = true;
            else if (arg.equals("noSched"))
                noSched = true;
            else if (arg.length() == 3)
            {
                cifDay = arg;
                fullCif = false;
            }
        }

        if (noSched && !doSmartCorpus)
        {
            System.out.println("Nothing to do, exiting");
            return;
        }

        try
        {
            File authFile = new File(new File(System.getProperty("user.home", "C:"), ".easigmap"), "config.json");
            AUTH = new JSONObject(new String(Files.readAllBytes(authFile.toPath())));
        }
        catch (IOException ex)
        {
            ex.printStackTrace();
            System.exit(-1);
        }

        System.out.println("Connecting to database...");
        Connection conn = null;
        try
        {
            conn = DriverManager.getConnection("jdbc:mariadb://localhost:3306/sigmaps?autoReconnect=true",AUTH.getString("DBUser"),AUTH.getString("DBPassword"));
        }
        catch (SQLException ex)
        {
            ex.printStackTrace();
            System.exit(-1);
        }

        try
        {
            if (doSmartCorpus)
            {
                System.out.println("Downloading and processing CORPUS dataset...");
                try (BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(downloadData("corpus"))))))
                {
                    String line = br.readLine();
                    JSONObject obj = new JSONObject(line);
                    JSONArray data = obj.getJSONArray("TIPLOCDATA");

                    PreparedStatement ps = conn.prepareStatement("INSERT INTO corpus (tiploc, stanox, tps_name, crs) " +
                            "VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE stanox=VALUES(stanox), crs=VALUES(crs)");
                    for (Object l : data)
                    {
                        JSONObject loc = (JSONObject) l;
                        if (loc.has("TIPLOC") && !loc.getString("TIPLOC").trim().isEmpty() && (
                                loc.has("STANOX") && !loc.getString("STANOX").trim().isEmpty() ||
                                loc.has("NLCDESC") && !loc.getString("NLCDESC").trim().isEmpty() ||
                                loc.has("3ALPHA") && !loc.getString("3ALPHA").trim().isEmpty()))
                        {
                            ps.setString(1, loc.getString("TIPLOC"));
                            ps.setString(2, loc.getString("STANOX"));
                            ps.setString(3, loc.getString("NLCDESC"));
                            ps.setString(4, loc.getString("3ALPHA"));
                            ps.addBatch();
                        }
                    }
                    System.out.print("Executing batch SQL... ");
                    System.out.println(IntStream.of(ps.executeBatch()).sum());
                }
                catch (IOException e) { e.printStackTrace(); }

                System.out.println("Downloading and processing SMART dataset...");
                try (BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(downloadData("smart"))))))
                {
                    String line = br.readLine();

                    JSONObject obj = new JSONObject(line);
                    JSONArray data = obj.getJSONArray("BERTHDATA");

                    PreparedStatement ps = conn.prepareStatement("DELETE from smart WHERE manual = 0;");
                    ps.executeUpdate();

                    ps = conn.prepareStatement("INSERT INTO smart (stanox, td, reports) VALUES (?,?,1) ON DUPLICATE KEY UPDATE reports=1");
                    for (Object l : data)
                    {
                        JSONObject loc = (JSONObject) l;
                        if (loc.has("STANOX") && !loc.getString("STANOX").trim().isEmpty() &&
                            loc.has("TD") && !loc.getString("TD").trim().isEmpty() &&
                            isInteger(loc.getString("STANOX")))
                        {
                            ps.setString(1, loc.getString("STANOX"));
                            ps.setString(2, loc.getString("TD"));
                            ps.addBatch();
                        }
                    }
                    System.out.print("Executing batch SQL... ");
                    System.out.println(IntStream.of(ps.executeBatch()).sum());
                }
                catch (IOException e) { e.printStackTrace(); }
            }

            if (!noSched)
            {
                System.out.println("Downloading and extracting CIF dataset...");

                File cifFile = new File(System.getProperty("java.io.tmpdir"), new SimpleDateFormat("yyyy-MM-dd").format(new Date()) + "-toc-" + (fullCif ? "full" : cifDay) + ".CIF");
                try (GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(downloadData(fullCif ? "cif-full" : ("cif-update-" + cifDay))));
                    FileOutputStream fos = new FileOutputStream(cifFile))
                {
                    byte[] buffer = new byte[8192];
                    int len;
                    while ((len = gzis.read(buffer)) != -1)
                        fos.write(buffer, 0, len);
                }
                catch (IOException ex)
                {
                    System.err.println("Failed to unzip CIF file");
                    ex.printStackTrace();
                    System.exit(1);
                }

                System.out.println("Processing CIF...");
                try (BufferedReader br = new BufferedReader(new FileReader(cifFile)))
                {
                    int count = 0;
                    int errcount = 0;
                    String line;
                    CIFRecord record = null;
                    CIFBSRecord recordBS = null;
                    List<CIFLocRecord> schedule = new ArrayList<>();

                    PreparedStatement psHD = conn.prepareStatement("INSERT INTO cif_files (current_ref, last_ref, date, update_type, error_count) VALUES(?,?,?,?,?)");
                    PreparedStatement psTI = conn.prepareStatement("INSERT INTO corpus (tiploc, stanox, tps_name, crs) VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE stanox=VALUES(stanox), tps_name=VALUES(tps_name), crs=VALUES(crs)");
                    PreparedStatement psBS1 = conn.prepareStatement("INSERT INTO schedules (schedule_uid, date_from, date_to, stp_indicator, schedule_source, days_run, identity) VALUES (?,?,?,?,'C',?,?)");
                    PreparedStatement psBS2 = conn.prepareStatement("UPDATE schedules SET runs_mon=?, runs_tue=?, runs_wed=?, runs_thu=?, runs_fri=?, runs_sat=?, runs_sun=?, over_midnight=? WHERE schedule_uid=? AND date_from=? AND stp_indicator=? AND schedule_source='C'");
                    PreparedStatement psBSDelLocs = conn.prepareStatement("DELETE FROM schedule_locations WHERE schedule_uid = ? AND stp_indicator = ? AND date_from = ? AND schedule_source = 'C'");
                    PreparedStatement psBSDelScheds = conn.prepareStatement("DELETE FROM schedules WHERE schedule_uid = ? AND stp_indicator = ? AND date_from = ? AND schedule_source = 'C'");
                    PreparedStatement psLoc = conn.prepareStatement("INSERT INTO schedule_locations (schedule_uid, date_from, stp_indicator, schedule_source, tiploc, scheduled_arrival, scheduled_departure, scheduled_pass, plat, line, path, activity, eng, pth, prf, type, loc_index) VALUES (?,?,?,'C',?,?,?,?,?,?,?,?,?,?,?,?,?)");

                    Calendar c = Calendar.getInstance();
                    c.set(Calendar.DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH)-1);
                    String yesterdayDMY = new SimpleDateFormat("ddMMyy").format(c.getTime());
                    String yesterdayYMD = new SimpleDateFormat("yyMMdd").format(c.getTime());

                    long countTI = 0;
                    long countBSDelLocs = 0;
                    long countBSDelScheds = 0;
                    long countBS1 = 0;
                    long countBS2 = 0;
                    long countLoc = 0;

                    try
                    {
                        while ((line = br.readLine()) != null)
                        {
                            try
                            {
                                record = CIFRecord.of(line);
                                switch(record.type)
                                {
                                    case HD:
                                    {
                                        CIFHDRecord recordHD = (CIFHDRecord) record;

                                        if (isAuto)
                                        {
                                            if (!recordHD.dateOfExtract.equals(yesterdayDMY))
                                            {
                                                System.err.println(recordHD.dateOfExtract + " is not the expected date (" + yesterdayDMY + ")");
                                                System.exit(2);
                                            }
                                        }

                                        PreparedStatement ps = conn.prepareStatement("SELECT current_ref FROM cif_files WHERE date=? AND update_type=?");
                                        ps.setString(1, recordHD.dateOfExtract);
                                        ps.setString(2, fullCif ? "F" : "U");
                                        ResultSet rs = ps.executeQuery();
                                        if (rs.first())
                                        {
                                            (isAuto ? System.out : System.err).println("Already processed " + recordHD.dateOfExtract + " (" + yesterdayDMY + ")");
                                            System.exit(3);
                                        }

                                        if (fullCif)
                                        {
                                            conn.prepareStatement("DELETE FROM schedules WHERE schedule_source='C'").execute();
                                            conn.prepareStatement("DELETE FROM schedule_locations WHERE schedule_source='C'").execute();
                                        }
                                        else
                                        {
                                            System.out.print("Deleting expired (" + yesterdayYMD + ") schedules... ");

                                            PreparedStatement psCFDel3 = conn.prepareStatement("DELETE schedules, schedule_locations FROM schedules "
                                                + "LEFT JOIN schedule_locations ON schedules.schedule_uid=schedule_locations.schedule_uid AND "
                                                + "schedules.date_from=schedule_locations.date_from AND "
                                                + "schedules.stp_indicator=schedule_locations.stp_indicator AND "
                                                + "schedules.schedule_source=schedule_locations.schedule_source "
                                                + "WHERE CAST(schedules.date_to AS INT) < ?");
                                            psCFDel3.setInt(1, Integer.parseInt(yesterdayYMD));
                                            System.out.println(psCFDel3.executeUpdate());
                                        }

                                        psHD.setString(1, recordHD.currentFileReference);
                                        psHD.setString(2, recordHD.lastFileReference);
                                        psHD.setString(3, recordHD.dateOfExtract);
                                        psHD.setString(4, recordHD.updateIndicator);

                                        System.out.println("Processing new schedules...");
                                        break;
                                    }
                                    case TI:
                                    {
                                        CIFTIRecord recordTI = (CIFTIRecord) record;
                                        psTI.setString(1, recordTI.tiploc);
                                        psTI.setString(2, recordTI.stanox);
                                        psTI.setString(3, recordTI.tpsDescription);
                                        psTI.setString(4, recordTI.threeAlphaCode);
                                        psTI.addBatch();
                                        break;
                                    }
                                    case TA:
                                    {
                                        CIFTARecord recordTA = (CIFTARecord) record;
                                        psTI.setString(1, recordTA.newTiploc);
                                        psTI.setString(2, recordTA.stanox);
                                        psTI.setString(3, recordTA.tpsDescription);
                                        psTI.setString(4, recordTA.threeAlphaCode);
                                        psTI.addBatch();
                                        break;
                                    }
                                    case BS:
                                    {
                                        if (schedule.size() > 0)
                                            throw new IllegalStateException("Unfinished schedule " + schedule.get(0).toString());
                                        recordBS = (CIFBSRecord) record;
                                        if ("D".equals(recordBS.transactionType) || "R".equals(recordBS.transactionType))
                                        {
                                            psBSDelLocs.setString(1, recordBS.trainUID);
                                            psBSDelLocs.setString(2, recordBS.stpIndicator);
                                            psBSDelLocs.setString(3, recordBS.dateRunsFrom);
                                            psBSDelLocs.addBatch();
                                            psBSDelScheds.setString(1, recordBS.trainUID);
                                            psBSDelScheds.setString(2, recordBS.stpIndicator);
                                            psBSDelScheds.setString(3, recordBS.dateRunsFrom);
                                            psBSDelScheds.addBatch();
                                        }

                                        if ("N".equals(recordBS.transactionType) || "R".equals(recordBS.transactionType))
                                        {
                                            psBS1.setString(1, recordBS.trainUID);
                                            psBS1.setString(2, recordBS.dateRunsFrom);
                                            psBS1.setString(3, recordBS.dateRunsTo);
                                            psBS1.setString(4, recordBS.stpIndicator);
                                            psBS1.setString(5, recordBS.daysRun);
                                            psBS1.setString(6, recordBS.trainIdentity);
                                            psBS1.addBatch();
                                        }

                                        if ("C".equals(recordBS.stpIndicator))
                                        {
                                            for (int i = 0; i < 7; i++)
                                                psBS2.setBoolean(i+1, recordBS.daysRun.charAt(i) == '1');

                                            psBS2.setBoolean(8, false);
                                            psBS2.setString(9, recordBS.trainUID);
                                            psBS2.setString(10, recordBS.dateRunsFrom);
                                            psBS2.setString(11, recordBS.stpIndicator);
                                            psBS2.addBatch();
                                        }

                                        break;
                                    }
                                    case LO:
                                    {
                                        CIFLORecord recordLO = (CIFLORecord) record;
                                        psLoc.setString(1, recordBS.trainUID);
                                        psLoc.setString(2, recordBS.dateRunsFrom);
                                        psLoc.setString(3, recordBS.stpIndicator);
                                        psLoc.setString(4, recordLO.getLocation());
                                        psLoc.setString(5, "");
                                        psLoc.setString(6, recordLO.scheduledDepartureTime);
                                        psLoc.setString(7, "");
                                        psLoc.setString(8, recordLO.platform);
                                        psLoc.setString(9, recordLO.line);
                                        psLoc.setString(10, "");
                                        psLoc.setString(11, recordLO.activity);
                                        psLoc.setString(12, recordLO.engineeringAllowance);
                                        psLoc.setString(13, recordLO.pathingAllowance);
                                        psLoc.setString(14, recordLO.performanceAllowance);
                                        psLoc.setString(15, "O");
                                        psLoc.setInt(16, 0);
                                        psLoc.addBatch();
                                        schedule.add(recordLO);
                                        break;
                                    }
                                    case LI:
                                    {
                                        CIFLIRecord recordLI = (CIFLIRecord) record;
                                        psLoc.setString(1, recordBS.trainUID);
                                        psLoc.setString(2, recordBS.dateRunsFrom);
                                        psLoc.setString(3, recordBS.stpIndicator);
                                        psLoc.setString(4, recordLI.getLocation());
                                        psLoc.setString(5, recordLI.scheduledArrivalTime);
                                        psLoc.setString(6, recordLI.scheduledDepartureTime);
                                        psLoc.setString(7, recordLI.scheduledPassTime);
                                        psLoc.setString(8, recordLI.platform);
                                        psLoc.setString(9, recordLI.line);
                                        psLoc.setString(10, recordLI.path);
                                        psLoc.setString(11, recordLI.activity);
                                        psLoc.setString(12, recordLI.engineeringAllowance);
                                        psLoc.setString(13, recordLI.pathingAllowance);
                                        psLoc.setString(14, recordLI.performanceAllowance);
                                        psLoc.setString(15, "I");
                                        psLoc.setInt(16, schedule.size());
                                        psLoc.addBatch();
                                        schedule.add(recordLI);
                                        break;
                                    }
                                    case LT:
                                    {
                                        CIFLTRecord recordLT = (CIFLTRecord) record;
                                        psLoc.setString(1, recordBS.trainUID);
                                        psLoc.setString(2, recordBS.dateRunsFrom);
                                        psLoc.setString(3, recordBS.stpIndicator);
                                        psLoc.setString(4, recordLT.getLocation());
                                        psLoc.setString(5, recordLT.scheduledArrivalTime);
                                        psLoc.setString(6, "");
                                        psLoc.setString(7, "");
                                        psLoc.setString(8, recordLT.platform);
                                        psLoc.setString(9, "");
                                        psLoc.setString(10, recordLT.path);
                                        psLoc.setString(11, recordLT.activity);
                                        psLoc.setString(12, "");
                                        psLoc.setString(13, "");
                                        psLoc.setString(14, "");
                                        psLoc.setString(15, "T");
                                        psLoc.setInt(16, schedule.size());
                                        psLoc.addBatch();
                                        schedule.add(recordLT);

                                        for (int i = 0; i < 7; i++)
                                            psBS2.setBoolean(i+1, recordBS.daysRun.charAt(i) == '1');

                                        CIFLORecord recordLO = (CIFLORecord) schedule.get(0);
                                        psBS2.setBoolean(8, Double.parseDouble(recordLO.scheduledDepartureTime.replace("H", ".5")) >
                                            Double.parseDouble(recordLT.scheduledArrivalTime.replace("H", ".5")));

                                        psBS2.setString(9, recordBS.trainUID);
                                        psBS2.setString(10, recordBS.dateRunsFrom);
                                        psBS2.setString(11, recordBS.stpIndicator);
                                        psBS2.addBatch();
                                        schedule.clear();
                                        break;
                                    }
                                    case ZZ:
                                    {
                                        br.mark(82);
                                        if (br.readLine() != null)
                                        {
                                            br.reset();
                                            System.err.println("Found ZZ record but more data found");
                                        }
                                        break;
                                    }
                                }
                            }
                            catch (SQLException sqlex)
                            {
                                System.err.println("Error no:      " + (++errcount));
                                System.err.println("Record raw:    " + line);
                                System.err.println("Record parsed: " + record);
                                System.err.println("Count:         " + (count+1));

                                sqlex.printStackTrace();
                            }
                            count++;

                            if (count % 10000 == 0)
                            {
                                countTI += LongStream.of(psTI.executeLargeBatch()).sum();
                                countBSDelLocs += LongStream.of(psBSDelLocs.executeLargeBatch()).sum();
                                countBSDelScheds += LongStream.of(psBSDelScheds.executeLargeBatch()).sum();
                                countBS1 += LongStream.of(psBS1.executeLargeBatch()).sum();
                                countBS2 += LongStream.of(psBS2.executeLargeBatch()).sum();
                                countLoc += LongStream.of(psLoc.executeLargeBatch()).sum();
                            }
                        }

                        if (count % 10000 != 0)
                        {
                            countTI += LongStream.of(psTI.executeLargeBatch()).sum();
                            countBSDelLocs += LongStream.of(psBSDelLocs.executeLargeBatch()).sum();
                            countBSDelScheds += LongStream.of(psBSDelScheds.executeLargeBatch()).sum();
                            countBS1 += LongStream.of(psBS1.executeLargeBatch()).sum();
                            countBS2 += LongStream.of(psBS2.executeLargeBatch()).sum();
                            countLoc += LongStream.of(psLoc.executeLargeBatch()).sum();
                        }

                        System.out.println(String.format("TI: %d, BSDelLocs: %d, BSDelScheds: %d, BS1: %d, BS2: %d, Loc: %d",
                                           countTI, countBSDelLocs, countBSDelScheds, countBS1, countBS2, countLoc));

                        System.out.print("Deleting expired activations... ");
                        PreparedStatement psActivations = conn.prepareStatement("DELETE FROM activations WHERE last_update < ? AND last_update != 0");
                        psActivations.setLong(1, System.currentTimeMillis() - 172800000L); // 48 hrs ago
                        System.out.println(psActivations.executeUpdate());
                    }
                    catch (SQLException sqlex)
                    {
                        errcount++;
                        System.out.println();
                        sqlex.printStackTrace();
                    }


                    if (record == null || record.type != CIFRecordType.ZZ)
                        System.err.println("Reached end of file without ZZ record, data may be incomplete");
                    else
                    {
                        psHD.setInt(5, errcount);
                        psHD.executeUpdate();
                    }
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }
        catch (SQLException ex)
        {
            ex.printStackTrace();
        }

        try { conn.close(); }
        catch (SQLException ex) { ex.printStackTrace(); }

        long time = System.currentTimeMillis() - start;
        System.out.printf("Done in %02d:%02d:%02d.%d (%d)%n", (time / 3600000) % 60, (time / 60000) % 60, (time / 1000) % 60, time % 1000, time);
    }

    private static File downloadData(String type) throws IOException
    {
        File file;
        String url;
        if ("corpus".equalsIgnoreCase(type))
        {
            file = new File(System.getProperty("java.io.tmpdir"), new SimpleDateFormat("yyyy-MM-dd").format(new Date()) + "-CORPUSExtract.json.gz");
            url = "https://datafeeds.networkrail.co.uk/ntrod/SupportingFileAuthenticate?type=CORPUS";
        }
        else if ("smart".equals(type))
        {
            file = new File(System.getProperty("java.io.tmpdir"), new SimpleDateFormat("yyyy-MM-dd").format(new Date()) + "-SMARTExtract.json.gz");
            url = "https://datafeeds.networkrail.co.uk/ntrod/SupportingFileAuthenticate?type=SMART";
        }
        else if ("cif-full".equals(type))
        {
            file = new File(System.getProperty("java.io.tmpdir"), new SimpleDateFormat("yyyy-MM-dd").format(new Date()) + "-toc-full.CIF.gz");
            url = "https://datafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_FULL_DAILY&day=toc-full.CIF.gz";
        }
        else if (type.startsWith("cif-update"))
        {
            String day = type.substring(11).toLowerCase();
            if (!Arrays.asList("sun","mon","tue","wed","thu","fri","sat").contains(day))
                throw new IllegalArgumentException(day + " is not a valid day");

            file = new File(System.getProperty("java.io.tmpdir"), new SimpleDateFormat("yyyy-MM-dd").format(new Date()) + "-toc-daily-" + day + ".CIF.gz");
            url = "https://datafeeds.networkrail.co.uk/ntrod/CifFileAuthenticate?type=CIF_ALL_UPDATE_DAILY&day=toc-update-" + day + ".CIF.gz";
        }
        else
            throw new IllegalArgumentException("'" + type + "' is not a downloadable file type");

        InputStream in;
        if (!file.exists())
        {
           HttpsURLConnection con = (HttpsURLConnection) new URL(url).openConnection();
            con.setRequestProperty("Authorization", "Basic " + DatatypeConverter.printBase64Binary((AUTH.getString("NROD_Username") + ":" + AUTH.getString("NROD_Password")).getBytes()).trim()); // Login details
            con.setInstanceFollowRedirects(false);
            System.out.println("Response: " + con.getResponseCode() + " " + con.getResponseMessage());
            if (con.getResponseCode() == HttpsURLConnection.HTTP_MOVED_TEMP || con.getResponseCode() == HttpsURLConnection.HTTP_MOVED_PERM)
            {
                String newLocation = con.getHeaderField("Location");
                System.out.println("Redirected to: " + newLocation);
                con = (HttpsURLConnection) new URL(newLocation).openConnection();
            }

            InputStream errIn = con.getErrorStream();
            if (con.getErrorStream() != null)
            {
                Scanner s = new Scanner(errIn).useDelimiter("\\A");
                if (s.hasNext())
                    System.err.println(s.next());
            }

            in = con.getInputStream();

            Files.copy(in, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
            in.close();
            System.out.println("File downloaded to " + file.getAbsolutePath());

            return file;
        }
        else
        {
            System.out.println("Using existing file");
            return file;
        }
    }

    private static boolean isInteger(String s)
    {
        if (s.isEmpty())
            return false;
        for (int i = 0; i < s.length(); i++)
        {
            if(i == 0 && s.charAt(i) == '-')
            {
                if(s.length() == 1)
                    return false;
                else
                    continue;
            }
            if(Character.digit(s.charAt(i),10) < 0)
                return false;
        }
        return true;
    }

    //<editor-fold defaultstate="collapsed" desc="CIF Types">
    static class CIFRecord
    {
        final CIFRecordType type;

        CIFRecord(CIFRecordType type)
        {
            this.type = type;
        }

        static CIFRecord of(String record)
        {
            CIFRecordType type = CIFRecordType.valueOf(record.substring(0, 2));

            switch(type)
            {
                case HD:
                    return new CIFHDRecord(record);
                case TI:
                    return new CIFTIRecord(record);
                case TA:
                    return new CIFTARecord(record);
                case TD:
                    return new CIFTDRecord(record);
                case AA:
                    return new CIFAARecord(record);
                case BS:
                    return new CIFBSRecord(record);
                case BX:
                    return new CIFBXRecord(record);
                case LO:
                    return new CIFLORecord(record);
                case LI:
                    return new CIFLIRecord(record);
                case LT:
                    return new CIFLTRecord(record);
                case CR:
                    return new CIFCRRecord(record);
                case ZZ:
                    return new CIFZZRecord();
                default:
                    throw new IllegalArgumentException(record.substring(0, 2) + " is not a valid CIF record type");
            }
        }
    }

    static class CIFHDRecord extends CIFRecord
    {
      //static final int[] lengths = {2, 20, 6, 4, 7, 7, 1, 1, 6, 6, 20};
        static final int[] offsets = {2, 22, 28, 32, 39, 46, 47, 48, 54, 60, 80};
        final String fileMainframeIdentity;
        final String dateOfExtract;
        final String timeOfExtract;
        final String currentFileReference;
        final String lastFileReference;
        final String updateIndicator;
        final String version;
        final String userStartDate;
        final String userEndDate;

        CIFHDRecord(String record)
        {
            super(CIFRecordType.HD);

            int i = 0;
            fileMainframeIdentity = record.substring(offsets[i], offsets[++i]);
            dateOfExtract = record.substring(offsets[i], offsets[++i]);
            timeOfExtract = record.substring(offsets[i], offsets[++i]);
            currentFileReference = record.substring(offsets[i], offsets[++i]);
            lastFileReference = record.substring(offsets[i], offsets[++i]);
            updateIndicator = record.substring(offsets[i], offsets[++i]);
            version = record.substring(offsets[i], offsets[++i]);
            userStartDate = record.substring(offsets[i], offsets[++i]);
            userEndDate = record.substring(offsets[i], offsets[++i]);
        }

        @Override
        public String toString()
        {
            return String.format("HD,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                                 fileMainframeIdentity, dateOfExtract,
                                 timeOfExtract, currentFileReference,
                                 lastFileReference, updateIndicator,
                                 version, userStartDate, userEndDate);
        }
    }

    static class CIFTIRecord extends CIFRecord
    {
      //static final int[] lengths = {2, 7, 2, 6, 1, 26, 5, 4, 3, 16, 8};
        static final int[] offsets = {2, 9, 11, 17, 18, 44, 49, 53, 56, 72, 80};
        final String tiploc;
        final String capitalsIdentification;
        final String nlc;
        final String nlcCheckChar;
        final String tpsDescription;
        final String stanox;
        final String poMcpCode;
        final String threeAlphaCode;
        final String nlcDescription;

        CIFTIRecord(String record)
        {
            super(CIFRecordType.TI);

            int i = 0;
            tiploc = record.substring(offsets[i], offsets[++i]);
            capitalsIdentification = record.substring(offsets[i], offsets[++i]);
            nlc = record.substring(offsets[i], offsets[++i]);
            nlcCheckChar = record.substring(offsets[i], offsets[++i]);
            tpsDescription = record.substring(offsets[i], offsets[++i]);
            stanox = record.substring(offsets[i], offsets[++i]);
            poMcpCode = record.substring(offsets[i], offsets[++i]);
            threeAlphaCode = record.substring(offsets[i], offsets[++i]);
            nlcDescription = record.substring(offsets[i], offsets[++i]);
        }

        @Override
        public String toString()
        {
            return String.format("TI,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                                 tiploc, capitalsIdentification,
                                 nlc, nlcCheckChar, tpsDescription, stanox,
                                 poMcpCode, threeAlphaCode, nlcDescription);
        }
    }

    static class CIFTARecord extends CIFRecord
    {
      //static final int[] lengths = {2, 7, 2, 6, 1, 26, 5, 4, 3, 16, 7, 1};
        static final int[] offsets = {2, 9, 11, 17, 18, 44, 49, 53, 56, 72, 79, 80};
        final String tiploc;
        final String capitalsIdentification;
        final String nlc;
        final String nlcCheckChar;
        final String tpsDescription;
        final String stanox;
        final String poMcpCode;
        final String threeAlphaCode;
        final String nlcDescription;
        final String newTiploc;

        CIFTARecord(String record)
        {
            super(CIFRecordType.TA);

            int i = 0;
            tiploc = record.substring(offsets[i], offsets[++i]);
            capitalsIdentification = record.substring(offsets[i], offsets[++i]);
            nlc = record.substring(offsets[i], offsets[++i]);
            nlcCheckChar = record.substring(offsets[i], offsets[++i]);
            tpsDescription = record.substring(offsets[i], offsets[++i]);
            stanox = record.substring(offsets[i], offsets[++i]);
            poMcpCode = record.substring(offsets[i], offsets[++i]);
            threeAlphaCode = record.substring(offsets[i], offsets[++i]);
            nlcDescription = record.substring(offsets[i], offsets[++i]);
            newTiploc = record.substring(offsets[i], offsets[++i]);
        }

        @Override
        public String toString()
        {
            return String.format("TA,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                                 tiploc, capitalsIdentification, nlc,
                                 nlcCheckChar, tpsDescription, stanox, poMcpCode,
                                 threeAlphaCode, nlcDescription, newTiploc);
        }
    }

    static class CIFTDRecord extends CIFRecord
    {
      //static final int[] lengths = {2, 7, 71};
        static final int[] offsets = {2, 9, 80};
        final String tiploc;

        CIFTDRecord(String record)
        {
            super(CIFRecordType.TD);

            tiploc = record.substring(offsets[0], offsets[1]);
        }

        @Override
        public String toString()
        {
            return String.format("TD,%s", tiploc);
        }
    }

    static class CIFAARecord extends CIFRecord
    {
      //static final int[] lengths = {2, 1, 6, 6, 6, 6, 7, 2, 1, 7, 1, 1, 1, 1, 31, 1};
        static final int[] offsets = {2, 3, 9, 15, 21, 27, 34, 36, 37, 44, 45, 46, 47, 48, 79, 80};
        final String transactionType;
        final String baseUID;
        final String assocUID;
        final String assocUIDStartDate;
        final String assocUIDEndDate;
        final String assocDays;
        final String assocCat;
        final String assocDateInd;
        final String assocLocation;
        final String baseSuffixLocation;
        final String assocSuffixLocation;
        final String diagramType;
        final String associationType;
        final String stpIndicator;

        CIFAARecord(String record)
        {
            super(CIFRecordType.AA);

            int i = 0;
            transactionType = record.substring(offsets[i], offsets[++i]);
            baseUID = record.substring(offsets[i], offsets[++i]);
            assocUID = record.substring(offsets[i], offsets[++i]);
            assocUIDStartDate = record.substring(offsets[i], offsets[++i]);
            assocUIDEndDate = record.substring(offsets[i], offsets[++i]);
            assocDays = record.substring(offsets[i], offsets[++i]);
            assocCat = record.substring(offsets[i], offsets[++i]);
            assocDateInd = record.substring(offsets[i], offsets[++i]);
            assocLocation = record.substring(offsets[i], offsets[++i]);
            baseSuffixLocation = record.substring(offsets[i], offsets[++i]);
            assocSuffixLocation = record.substring(offsets[i], offsets[++i]);
            diagramType = record.substring(offsets[i], offsets[++i]);
            associationType = record.substring(offsets[i], offsets[++i]);
            ++i;
            stpIndicator = record.substring(offsets[i], offsets[++i]);
        }

        @Override
        public String toString()
        {
            return String.format("AA,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                                 transactionType, baseUID, assocUID,
                                 assocUIDStartDate, assocUIDEndDate, assocDays,
                                 assocCat, assocDateInd, assocLocation,
                                 baseSuffixLocation, assocSuffixLocation,
                                 diagramType, associationType, stpIndicator);
        }
    }

    static class CIFBSRecord extends CIFRecord
    {
      //static final int[] lengths = {2, 1, 6, 6, 6, 7, 1, 1, 2, 4, 4, 1, 8, 1, 3, 4, 3, 6, 1, 1, 1, 1, 4, 4, 1, 1};
        static final int[] offsets = {2, 3, 9, 15, 21, 28, 29, 30, 32, 36, 40, 41, 49, 50, 53, 57, 60, 66, 67, 68, 69, 70, 74, 78, 79, 80};
        final String transactionType;
        final String trainUID;
        final String dateRunsFrom;
        final String dateRunsTo;
        final String daysRun;
        final String bankHolidayRunning;
        final String trainStatus;
        final String trainCategory;
        final String trainIdentity;
        final String headcode;
        final String courseIndicator;
        final String trainServiceCode;
        final String businessSector;
        final String powerType;
        final String timingLoad;
        final String speed;
        final String operatingChars;
        final String trainClass;
        final String sleepers;
        final String reservations;
        final String connectIndicator;
        final String cateringCode;
        final String serviceBranding;
        final String stpIndicator;

        CIFBSRecord(String record)
        {
            super(CIFRecordType.BS);

            int i = 0;
            transactionType = record.substring(offsets[i], offsets[++i]);
            trainUID = record.substring(offsets[i], offsets[++i]);
            dateRunsFrom = record.substring(offsets[i], offsets[++i]);
            dateRunsTo = record.substring(offsets[i], offsets[++i]);
            daysRun = record.substring(offsets[i], offsets[++i]);
            bankHolidayRunning = record.substring(offsets[i], offsets[++i]);
            trainStatus = record.substring(offsets[i], offsets[++i]);
            trainCategory = record.substring(offsets[i], offsets[++i]);
            trainIdentity = record.substring(offsets[i], offsets[++i]);
            headcode = record.substring(offsets[i], offsets[++i]);
            courseIndicator = record.substring(offsets[i], offsets[++i]);
            trainServiceCode = record.substring(offsets[i], offsets[++i]);
            businessSector = record.substring(offsets[i], offsets[++i]);
            powerType = record.substring(offsets[i], offsets[++i]);
            timingLoad = record.substring(offsets[i], offsets[++i]);
            speed = record.substring(offsets[i], offsets[++i]);
            operatingChars = record.substring(offsets[i], offsets[++i]);
            trainClass = record.substring(offsets[i], offsets[++i]);
            sleepers = record.substring(offsets[i], offsets[++i]);
            reservations = record.substring(offsets[i], offsets[++i]);
            connectIndicator = record.substring(offsets[i], offsets[++i]);
            cateringCode = record.substring(offsets[i], offsets[++i]);
            serviceBranding = record.substring(offsets[i], offsets[++i]);
            ++i;
            stpIndicator = record.substring(offsets[i], offsets[++i]);
        }

        @Override
        public String toString()
        {
            return String.format("BS,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                + "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                                 transactionType, trainUID, dateRunsFrom,
                                 dateRunsTo, daysRun, bankHolidayRunning,
                                 trainStatus, trainCategory, trainIdentity,
                                 headcode, courseIndicator, trainServiceCode,
                                 businessSector, powerType, timingLoad, speed,
                                 operatingChars, trainClass, sleepers,
                                 reservations, connectIndicator, cateringCode,
                                 serviceBranding, stpIndicator);
        }
    }

    static class CIFBXRecord extends CIFRecord
    {
      //static final int[] lengths = {2, 4, 5, 2, 1, 8, 1, 57};
        static final int[] offsets = {2, 6, 11, 13, 14, 22, 23, 80};
        final String tractionClass;
        final String uicCode;
        final String atocCode;
        final String applicableTimetableCode;
        final String retailTrainID;
        final String source;

        CIFBXRecord(String record)
        {
            super(CIFRecordType.BX);

            int i = 0;
            tractionClass = record.substring(offsets[i], offsets[++i]);
            uicCode = record.substring(offsets[i], offsets[++i]);
            atocCode = record.substring(offsets[i], offsets[++i]);
            applicableTimetableCode = record.substring(offsets[i], offsets[++i]);
            retailTrainID = record.substring(offsets[i], offsets[++i]);
            source = record.substring(offsets[i], offsets[++i]);
        }

        @Override
        public String toString()
        {
            return String.format("BX,%s,%s,%s,%s,%s,%s",
                                 tractionClass, uicCode, atocCode,
                                 applicableTimetableCode, retailTrainID,
                                 source);
        }
    }

    static class CIFLocRecord extends CIFRecord
    {
        private String location;

        CIFLocRecord(CIFRecordType type)
        {
            super(type);
        }

        String getLocation()
        {
            return location;
        }

        final void setLocation(String location)
        {
            this.location = location;
        }
    }

    static class CIFLORecord extends CIFLocRecord
    {
      //static final int[] lengths = {2, 7, 1, 5, 4, 3, 3, 2, 2, 12, 2, 37};
        static final int[] offsets = {2, 9, 10, 15, 19, 22, 25, 27, 29, 41, 43, 80};
      //public final String location;
        final String locationIndex;
        final String scheduledDepartureTime;
        final String publicDepartureTime;
        final String platform;
        final String line;
        final String engineeringAllowance;
        final String pathingAllowance;
        final String activity;
        final String performanceAllowance;

        CIFLORecord(String record)
        {
            super(CIFRecordType.LO);

            int i = 0;
            setLocation(record.substring(offsets[i], offsets[++i]));
            locationIndex = record.substring(offsets[i], offsets[++i]);
            scheduledDepartureTime = record.substring(offsets[i], offsets[++i]);
            publicDepartureTime = record.substring(offsets[i], offsets[++i]);
            platform = record.substring(offsets[i], offsets[++i]);
            line = record.substring(offsets[i], offsets[++i]);
            engineeringAllowance = record.substring(offsets[i], offsets[++i]);
            pathingAllowance = record.substring(offsets[i], offsets[++i]);
            activity = record.substring(offsets[i], offsets[++i]);
            performanceAllowance = record.substring(offsets[i], offsets[++i]);
        }

        @Override
        public String toString()
        {
            return String.format("LO,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                                 getLocation(), locationIndex,
                                 scheduledDepartureTime, publicDepartureTime,
                                 platform, line, engineeringAllowance,
                                 pathingAllowance, activity,
                                 performanceAllowance);
        }
    }

    static class CIFLIRecord extends CIFLocRecord
    {
      //static final int[] lengths = {2, 7, 1, 5, 5, 5, 4, 4, 3, 3, 3, 12, 2, 2, 2, 20};
        static final int[] offsets = {2, 9, 10, 15, 20, 25, 29, 33, 36, 39, 42, 54, 56, 58, 60, 80};
      //public final String location;
        final String locationIndex;
        final String scheduledArrivalTime;
        final String scheduledDepartureTime;
        final String scheduledPassTime;
        final String publicArrivalTime;
        final String publicDepartureTime;
        final String platform;
        final String line;
        final String path;
        final String activity;
        final String engineeringAllowance;
        final String pathingAllowance;
        final String performanceAllowance;


        CIFLIRecord(String record)
        {
            super(CIFRecordType.LI);

            int i = 0;
            setLocation(record.substring(offsets[i], offsets[++i]));
            locationIndex = record.substring(offsets[i], offsets[++i]);
            scheduledArrivalTime = record.substring(offsets[i], offsets[++i]);
            scheduledDepartureTime = record.substring(offsets[i], offsets[++i]);
            scheduledPassTime = record.substring(offsets[i], offsets[++i]);
            publicArrivalTime = record.substring(offsets[i], offsets[++i]);
            publicDepartureTime = record.substring(offsets[i], offsets[++i]);
            platform = record.substring(offsets[i], offsets[++i]);
            line = record.substring(offsets[i], offsets[++i]);
            path = record.substring(offsets[i], offsets[++i]);
            activity = record.substring(offsets[i], offsets[++i]);
            engineeringAllowance = record.substring(offsets[i], offsets[++i]);
            pathingAllowance = record.substring(offsets[i], offsets[++i]);
            performanceAllowance = record.substring(offsets[i], offsets[++i]);
        }

        @Override
        public String toString()
        {
            return String.format("LI,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s",
                                 getLocation(), locationIndex,
                                 scheduledArrivalTime, scheduledDepartureTime,
                                 scheduledPassTime, publicArrivalTime,
                                 publicDepartureTime, platform, line, path,
                                 activity, engineeringAllowance,
                                 pathingAllowance, performanceAllowance);
        }
    }

    static class CIFLTRecord extends CIFLocRecord
    {
      //static final int[] lengths = {2, 7, 1, 5, 4, 3, 3, 12, 43};
        static final int[] offsets = {2, 9, 10, 15, 19, 22, 25, 37, 80};
      //public final String location;
        final String locationIndex;
        final String scheduledArrivalTime;
        final String publicArrivalTime;
        final String platform;
        final String path;
        final String activity;

        CIFLTRecord(String record)
        {
            super(CIFRecordType.LT);

            int i = 0;
            setLocation(record.substring(offsets[i], offsets[++i]));
            locationIndex = record.substring(offsets[i], offsets[++i]);
            scheduledArrivalTime = record.substring(offsets[i], offsets[++i]);
            publicArrivalTime = record.substring(offsets[i], offsets[++i]);
            platform = record.substring(offsets[i], offsets[++i]);
            path = record.substring(offsets[i], offsets[++i]);
            activity = record.substring(offsets[i], offsets[++i]);
        }

        @Override
        public String toString()
        {
            return String.format("LT,%s,%s,%s,%s,%s,%s,%s",
                                 getLocation(), locationIndex,
                                 scheduledArrivalTime, publicArrivalTime,
                                 platform, path, activity);
        }
    }

    static class CIFCRRecord extends CIFRecord
    {
      //static final int[] lengths = {2, 7, 1, 2, 4, 4, 1, 8, 1, 3, 4, 3, 6, 1, 1, 1, 1, 4, 4, 4, 5, 8, 5};
        static final int[] offsets = {2, 9, 10, 12, 16, 20, 21, 29, 30, 33, 37, 40, 46, 47, 48, 49, 50, 54, 58, 62, 67, 75, 80};
        final String location;
        final String locationIndex;
        final String trainCategory;
        final String trainIdentity;
        final String headcode;
        final String courseIndicator;
        final String trainServiceCode;
        final String businessSector;
        final String powerType;
        final String timingLoad;
        final String speed;
        final String operatingChars;
        final String trainClass;
        final String sleepers;
        final String reservations;
        final String connectIndicator;
        final String cateringCode;
        final String serviceBranding;
        final String tractionClass;
        final String uicCode;
        final String retailTrainID;

        CIFCRRecord(String record)
        {
            super(CIFRecordType.CR);

            int i = 0;
            location = record.substring(offsets[i], offsets[++i]);
            locationIndex = record.substring(offsets[i], offsets[++i]);
            trainCategory = record.substring(offsets[i], offsets[++i]);
            trainIdentity = record.substring(offsets[i], offsets[++i]);
            headcode = record.substring(offsets[i], offsets[++i]);
            courseIndicator = record.substring(offsets[i], offsets[++i]);
            trainServiceCode = record.substring(offsets[i], offsets[++i]);
            businessSector = record.substring(offsets[i], offsets[++i]);
            powerType = record.substring(offsets[i], offsets[++i]);
            timingLoad = record.substring(offsets[i], offsets[++i]);
            speed = record.substring(offsets[i], offsets[++i]);
            operatingChars = record.substring(offsets[i], offsets[++i]);
            trainClass = record.substring(offsets[i], offsets[++i]);
            sleepers = record.substring(offsets[i], offsets[++i]);
            reservations = record.substring(offsets[i], offsets[++i]);
            connectIndicator = record.substring(offsets[i], offsets[++i]);
            cateringCode = record.substring(offsets[i], offsets[++i]);
            serviceBranding = record.substring(offsets[i], offsets[++i]);
            tractionClass = record.substring(offsets[i], offsets[++i]);
            uicCode = record.substring(offsets[i], offsets[++i]);
            retailTrainID = record.substring(offsets[i], offsets[++i]);
        }

        @Override
        public String toString()
        {
            return String.format("CR,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                + "%s,%s,%s,%s,%s,%s,%s",
                                 location, locationIndex, trainCategory,
                                 trainIdentity, headcode, courseIndicator,
                                 trainServiceCode, businessSector, powerType,
                                 timingLoad, speed, operatingChars, trainClass,
                                 sleepers, reservations, connectIndicator,
                                 cateringCode, serviceBranding, tractionClass,
                                 uicCode, retailTrainID);
        }

    }

    static class CIFZZRecord extends CIFRecord
    {
      //static final int[] lengths = {2, 78};

        CIFZZRecord()
        {
            super(CIFRecordType.ZZ);
        }

        @Override
        public String toString()
        {
            return "ZZ";
        }
    }

    public enum CIFRecordType
    {
        HD, TI, TA, TD, AA, BS, BX, LO, LI, LT, CR, ZZ
    }
    //</editor-fold>
}
