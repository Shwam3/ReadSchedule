package readschedule;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import javax.net.ssl.HttpsURLConnection;
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
import java.sql.Statement;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.stream.LongStream;
import java.util.zip.GZIPInputStream;

public class ReadSchedule
{
    private static JSONObject config = new JSONObject();
    private static final File TEMP_DIR = new File(System.getProperty("java.io.tmpdir"));

    public static void main(String[] args)
    {
        long start = System.currentTimeMillis();

        JSONObject options = new JSONObject();
        options.put("doSmartCorpus", false);
        options.put("fullCif", true);
        options.put("noSched", false);
        options.put("isAuto", false);
        options.put("force", false);
        options.put("cifDay", "");
        options.put("configDir", new File(System.getProperty("user.home", "C:"), ".easigmap").getAbsolutePath());

        if (args.length == 0)
        {
            System.err.println("Usage ReadSchedule [doSmartCorpus] [isAuto] [noSched|dow] [fullCif] [-c=config_dir]");
            return;
        }

        for (String arg : args)
        {
            if (arg.equalsIgnoreCase("doSmartCorpus"))
                options.put("doSmartCorpus", true);
            else if (arg.equalsIgnoreCase("isAuto"))
                options.put("isAuto", true);
            else if (arg.equalsIgnoreCase("noSched"))
                options.put("noSched", true);
            else if (arg.equalsIgnoreCase("force"))
                options.put("force", true);
            else if (arg.startsWith("-c="))
                options.put("configDir", arg.substring(3));
            else if (arg.length() == 3)
            {
                options.put("cifDay", arg);
                options.put("fullCif", false);
            }
        }

        if (options.getBoolean("noSched") && !options.getBoolean("doSmartCorpus"))
        {
            System.out.println("Nothing to do, exiting");
            return;
        }

        try
        {
            File authFile = new File(options.getString("configDir"), "config.json");
            config = new JSONObject(new String(Files.readAllBytes(authFile.toPath())));
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
            conn = DriverManager.getConnection("jdbc:mariadb://" + config.optString("DBLocation", "localhost:3306/sigmaps") +
                            "?autoReconnect=true&rewriteBatchedStatements=true",
                    config.getString("DBUser"), config.getString("DBPassword"));
            conn.setAutoCommit(false);
        }
        catch (SQLException ex)
        {
            ex.printStackTrace();
            System.exit(-1);
        }

        doSmartCorpus(conn, options);

        doSchedule(conn, options);

        try { conn.rollback(); }
        catch (SQLException ex) { ex.printStackTrace(); }

        try { conn.close(); }
        catch (SQLException ex) { ex.printStackTrace(); }

        long time = System.currentTimeMillis() - start;
        System.out.printf("Done in %02d:%02d:%02d.%d (%d)%n", (time / 3600000) % 60, (time / 60000) % 60, (time / 1000) % 60, time % 1000, time);
    }

    private static void doSmartCorpus(Connection conn, JSONObject config)
    {
        if (!config.getBoolean("doSmartCorpus"))
            return;

        System.out.println("Downloading and processing CORPUS dataset...");
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(downloadData("corpus"))))))
        {
            br.mark(1);
            char[] bom = new char[1];
            br.read(bom, 0, 1);
            if (bom[0] != '\uFEFF' && bom[0] != '\uFFEF')
                br.reset();

            JSONObject obj = new JSONObject(new JSONTokener(br));
            JSONArray data = obj.getJSONArray("TIPLOCDATA");

            PreparedStatement ps = conn.prepareStatement("INSERT INTO corpus (tiploc, stanox, tps_name, crs) " +
                    "VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE stanox=VALUES(stanox), crs=VALUES(crs)");
            for (Object l : data)
            {
                JSONObject loc = (JSONObject) l;
                if (loc.has("TIPLOC") && !loc.getString("TIPLOC").trim().isEmpty() &&
                        loc.has("STANOX") && !loc.getString("STANOX").trim().isEmpty())
                {
                    ps.setString(1, loc.getString("TIPLOC"));
                    ps.setString(2, loc.getString("STANOX"));
                    ps.setString(3, loc.getString("NLCDESC"));
                    ps.setString(4, loc.getString("3ALPHA"));
                    ps.addBatch();
                }
            }
            System.out.print("Executing batch SQL... ");
            System.out.println(LongStream.of(ps.executeLargeBatch())
                    .map(i -> i == Statement.SUCCESS_NO_INFO ? 1 : i).filter(i -> i > 0).sum());
            conn.commit();
        }
        catch (IOException | JSONException e) { e.printStackTrace(); }
        catch (SQLException e)
        {
            try { conn.rollback(); }
            catch (SQLException e2) { e.addSuppressed(e2); }

            e.printStackTrace();
        }

        System.out.println("Downloading and processing SMART dataset...");
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(downloadData("smart"))))))
        {
            br.mark(1);
            char[] bom = new char[1];
            br.read(bom, 0, 1);
            if (bom[0] != '\uFEFF' && bom[0] != '\uFFEF')
                br.reset();

            JSONObject obj = new JSONObject(new JSONTokener(br));
            JSONArray data = obj.getJSONArray("BERTHDATA");

            PreparedStatement ps = conn.prepareStatement("DELETE from smart WHERE manual=0");
            ps.executeUpdate();

            ps = conn.prepareStatement("INSERT INTO smart (stanox, td, reports, max_offset) VALUES (?,?,1,?) " +
                    "ON DUPLICATE KEY UPDATE reports=1, max_offset=GREATEST(max_offset, VALUES(max_offset))");
            for (Object l : data)
            {
                JSONObject loc = (JSONObject) l;
                if (loc.has("STANOX") && !loc.getString("STANOX").trim().isEmpty() &&
                        loc.has("TD") && !loc.getString("TD").trim().isEmpty() &&
                        isInteger(loc.getString("STANOX")))
                {
                    ps.setString(1, loc.getString("STANOX"));
                    ps.setString(2, loc.getString("TD"));
                    ps.setInt(3, Math.max(0,
                            (int) Math.ceil(Double.parseDouble(loc.getString("BERTHOFFSET")))) * 1000);
                    ps.addBatch();
                }
            }
            System.out.print("Executing batch SQL... ");
            System.out.println(LongStream.of(ps.executeLargeBatch())
                    .map(i -> i == Statement.SUCCESS_NO_INFO ? 1 : i).filter(i -> i > 0).sum());
            conn.commit();
        }
        catch (IOException e) { e.printStackTrace(); }
        catch (SQLException | JSONException e)
        {
            try { conn.rollback(); }
            catch (SQLException e2) { e.addSuppressed(e2); }

            e.printStackTrace();
        }
    }

    private static void doSchedule(Connection conn, JSONObject config)
    {
        if (config.getBoolean("noSched"))
            return;

        System.out.println("Downloading and extracting CIF dataset...");

        File cifFile = new File(TEMP_DIR,
                new SimpleDateFormat("yyyy-MM-dd").format(new Date()) + "-toc-"
                        + (config.getBoolean("fullCif") ? "full" : config.getString("cifDay")) + ".CIF");
        gUnzipCIF(cifFile, config.getBoolean("fullCif") ? "cif-full" : ("cif-update-" + config.getString("cifDay")));

        System.out.println("Processing CIF...");
        try (BufferedReader br = new BufferedReader(new FileReader(cifFile)))
        {
            boolean needsExec = false;
            List<CIFRecordType> canExec = Arrays.asList(CIFRecordType.HD, CIFRecordType.TI, CIFRecordType.TA,
                    CIFRecordType.TD, CIFRecordType.AA, CIFRecordType.LT, CIFRecordType.ZZ);
            int count = 0;
            int errcount = 0;
            String line;
            CIFRecord record = null;
            CIFBSRecord recordBS = null;
            List<CIFLocRecord> schedule = new ArrayList<>();

            PreparedStatement psHD = conn.prepareStatement("INSERT INTO cif_files (current_ref,last_ref,date,update_type,error_count) " +
                    "VALUES (?,?,?,?,?)");
            PreparedStatement psTI = conn.prepareStatement("INSERT INTO corpus (tiploc,stanox,tps_name,crs) VALUES (?,?,?,?) " +
                    "ON DUPLICATE KEY UPDATE stanox=VALUES(stanox), tps_name=VALUES(tps_name), crs=VALUES(crs)");
            PreparedStatement psBS = conn.prepareStatement("INSERT INTO schedules VALUES " +
                    "(?,?,?,?,?,'C',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            PreparedStatement psSchedCreateTime = conn.prepareStatement("SELECT creation_timestamp FROM schedules WHERE schedule_key=?");
            PreparedStatement psBSDelLocs = conn.prepareStatement("DELETE FROM schedule_locations WHERE schedule_key=?");
            PreparedStatement psBSDelScheds = conn.prepareStatement("DELETE FROM schedules WHERE schedule_key=?");
            PreparedStatement psBSDelCR = conn.prepareStatement("DELETE FROM change_en_route WHERE schedule_key=?");
            PreparedStatement psBSSafeDelLocs = conn.prepareStatement("DELETE FROM schedule_locations WHERE schedule_key=?");
            PreparedStatement psBSSafeDelScheds = conn.prepareStatement("DELETE FROM schedules WHERE schedule_key=?");
            PreparedStatement psBSSafeDelCR = conn.prepareStatement("DELETE FROM change_en_route WHERE schedule_key=?");
            PreparedStatement psLoc = conn.prepareStatement("INSERT INTO schedule_locations VALUES (?,?,?,?,'C',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            PreparedStatement psCR = conn.prepareStatement("INSERT INTO change_en_route VALUES (?,?,?,?,'C',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

            Calendar c = Calendar.getInstance();
            c.set(Calendar.DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH)-1);
            String yesterdayDMY = new SimpleDateFormat("ddMMyy").format(c.getTime());
            String yesterdayYMD = new SimpleDateFormat("yyMMdd").format(c.getTime());

            long countTI = 0;
            long countBSDelLocs = 0;
            long countBSDelScheds = 0;
            long countBSDelCR = 0;
            long countBSSafeDelLocs = 0;
            long countBSSafeDelScheds = 0;
            long countBSSafeDelCR = 0;
            long countBS = 0;
            long countLoc = 0;
            long countCR = 0;

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

                            if (config.getBoolean("isAuto"))
                            {
                                if (!recordHD.dateOfExtract.equals(yesterdayDMY))
                                {
                                    System.err.println(recordHD.dateOfExtract + " is not the expected date of " + yesterdayDMY);

                                    cifFile.deleteOnExit();
                                    System.exit(2);
                                }
                            }

                            try (PreparedStatement ps = conn.prepareStatement("SELECT current_ref FROM cif_files WHERE date=? AND update_type=?"))
                            {
                                ps.setString(1, recordHD.dateOfExtract);
                                ps.setString(2, config.getBoolean("fullCif") ? "F" : "U");
                                try (ResultSet rs = ps.executeQuery())
                                {
                                    if (rs.first())
                                    {
                                        (config.getBoolean("isAuto") ? System.out : System.err).println("Already processed " + recordHD.dateOfExtract + " (" + yesterdayDMY + ")");
                                        System.exit(3);
                                    }
                                }
                            }

                            if (config.getBoolean("fullCif"))
                            {
                                try (//PreparedStatement ps1 = conn.prepareStatement("INSERT INTO stored_timestamps SELECT schedule_key, creation_timestamp, revision_timestamp FROM schedules WHERE schedule_source='C' ON DUPLICATE KEY UPDATE creation_timestamp=VALUES(creation_timestamp), revision_timestamp=VALUES(revision_timestamp);");
                                     PreparedStatement ps2 = conn.prepareStatement("DELETE FROM schedules WHERE schedule_source='C'");
                                     PreparedStatement ps3 = conn.prepareStatement("DELETE FROM schedule_locations WHERE schedule_source='C'");
                                     PreparedStatement ps4 = conn.prepareStatement("DELETE FROM change_en_route WHERE schedule_source='C'")) {
                                    //System.out.print("Backing up timestamps...");
                                    //ps1.execute();
                                    //System.out.println();
                                    System.out.print("Deleting CIF data...");
                                    ps2.execute();
                                    ps3.execute();
                                    ps4.execute();
                                    System.out.println();
                                }
                            }
                            else
                            {
                                try (PreparedStatement ps = conn.prepareStatement("SELECT current_ref FROM cif_files WHERE date=? AND update_type='U'"))
                                {
                                    ps.setString(1, yesterdayDMY);
                                    try (ResultSet rs = ps.executeQuery())
                                    {
                                        if (rs.first())
                                        {
                                            if (!recordHD.lastFileReference.equals(rs.getString(1)))
                                            {
                                                if (!config.getBoolean("force"))
                                                {
                                                    System.err.println("File references don't match, 'force' arg required. Aborted. (" + recordHD.lastFileReference + "/" + rs.getString(1) + ")");
                                                    System.exit(4);
                                                }
                                                else
                                                {
                                                    System.out.println("File references don't match, forcing update anyway");
                                                }
                                            }
                                        }
                                    }
                                }

                                System.out.print("Deleting expired (" + yesterdayYMD + ") schedules... ");

                                PreparedStatement psCFDel3 = conn.prepareStatement("DELETE schedules, " +
                                        "schedule_locations, change_en_route FROM schedules " +
                                        "LEFT JOIN schedule_locations ON schedules.schedule_key=schedule_locations.schedule_key " +
                                        "LEFT JOIN change_en_route ON schedules.schedule_key=change_en_route.schedule_key " +
                                        "WHERE CAST(schedules.date_to AS INT) < ?");
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
                            if (!recordTI.tiploc.trim().isEmpty() && !recordTI.stanox.trim().isEmpty())
                            {
                                psTI.setString(1, recordTI.tiploc);
                                psTI.setString(2, recordTI.stanox);
                                psTI.setString(3, recordTI.tpsDescription);
                                psTI.setString(4, recordTI.threeAlphaCode);
                                psTI.addBatch();
                            }
                            break;
                        }
                        case TA:
                        {
                            CIFTARecord recordTA = (CIFTARecord) record;
                            if (!recordTA.tiploc.trim().isEmpty() && !recordTA.stanox.trim().isEmpty())
                            {
                                if (!recordTA.newTiploc.trim().isEmpty())
                                    psTI.setString(1, recordTA.newTiploc);
                                else
                                    psTI.setString(1, recordTA.tiploc);
                                psTI.setString(2, recordTA.stanox);
                                psTI.setString(3, recordTA.tpsDescription);
                                psTI.setString(4, recordTA.threeAlphaCode);
                                psTI.addBatch();
                            }
                            break;
                        }
                        case BS:
                        {
                            if (schedule.size() > 0)
                                throw new IllegalStateException("Unfinished schedule " + schedule.get(0).toString());
                            recordBS = (CIFBSRecord) record;

                            long creationTS = System.currentTimeMillis();
                            if ("R".equals(recordBS.transactionType))
                            {
                                psSchedCreateTime.setString(1, recordBS.scheduleKey);
                                try (ResultSet rs = psSchedCreateTime.executeQuery())
                                {
                                    if (rs.next())
                                        creationTS = rs.getLong(1) > 0 ? rs.getLong(1) : creationTS;
                                }
                            }

                            if ("D".equals(recordBS.transactionType) || "R".equals(recordBS.transactionType))
                            {
                                psBSDelLocs.setString(1, recordBS.scheduleKey);
                                psBSDelLocs.addBatch();
                                psBSDelScheds.setString(1, recordBS.scheduleKey);
                                psBSDelScheds.addBatch();
                                psBSDelCR.setString(1, recordBS.scheduleKey);
                                psBSDelCR.addBatch();
                            }
                            else if (config.getBoolean("isAuto") || config.getBoolean("force"))
                            {
                                // Always delete previous schedules to avoid update failures if isAuto, separate for tracking stats
                                psBSSafeDelLocs.setString(1, recordBS.scheduleKey);
                                psBSSafeDelLocs.addBatch();
                                psBSSafeDelScheds.setString(1, recordBS.scheduleKey);
                                psBSSafeDelScheds.addBatch();
                                psBSSafeDelCR.setString(1, recordBS.scheduleKey);
                                psBSSafeDelCR.addBatch();
                            }

                            if ("N".equals(recordBS.transactionType) || "R".equals(recordBS.transactionType))
                            {
                                psBS.setString(1, recordBS.scheduleKey);
                                psBS.setString(2, recordBS.trainUID);
                                psBS.setString(3, recordBS.dateRunsFrom);
                                psBS.setString(4, recordBS.dateRunsTo);
                                psBS.setString(5, recordBS.stpIndicator);
                                psBS.setString(6, recordBS.daysRun);
                                psBS.setString(7, recordBS.trainIdentity);
                                for (int i = 8; i <= 14; i++)
                                    psBS.setBoolean(i, recordBS.daysRun.charAt(i - 8) == '1');

                              //psBS.setBoolean(15, over_midnight); set in LT
                                psBS.setLong(16, creationTS);
                                if ("R".equals(recordBS.transactionType))
                                    psBS.setLong(17, System.currentTimeMillis());
                                else
                                    psBS.setNull(17, Types.BIGINT);
                                psBS.setString(18, recordBS.trainCategory);
                                psBS.setString(19, recordBS.trainStatus);
                                psBS.setString(20, recordBS.headcode);
                                psBS.setString(21, recordBS.trainServiceCode);
                                psBS.setString(22, recordBS.powerType);
                                psBS.setString(23, recordBS.timingLoad);
                                psBS.setString(24, recordBS.speed);
                                psBS.setString(25, recordBS.operatingChars);
                                psBS.setString(26, recordBS.trainClass);
                                psBS.setString(27, recordBS.sleepers);
                                psBS.setString(28, recordBS.reservations);
                                psBS.setString(29, recordBS.cateringCode);
                                psBS.setString(30, recordBS.serviceBranding);
                              //psBS.setString(31, toc_code); set in BX

                                if ("C".equals(recordBS.stpIndicator))
                                {
                                    psBS.setBoolean(15, false);
                                    psBS.setString(31, "");
                                    psBS.addBatch();
                                }
                            }

                            break;
                        }
                        case BX:
                        {
                            CIFBXRecord recordBX = (CIFBXRecord)record;
                            psBS.setString(31, recordBX.atocCode);

                            break;
                        }
                        case LO:
                        {
                            CIFLORecord recordLO = (CIFLORecord) record;
                            psLoc.setString(1, recordBS.scheduleKey);
                            psLoc.setString(2, recordBS.trainUID);
                            psLoc.setString(3, recordBS.dateRunsFrom);
                            psLoc.setString(4, recordBS.stpIndicator);
                            psLoc.setString(5, recordLO.getLocation());
                            psLoc.setString(6, "");
                            psLoc.setString(7, recordLO.scheduledDepartureTime);
                            psLoc.setString(8, "");
                            psLoc.setString(9, "");
                            psLoc.setString(10, recordLO.publicDepartureTime);
                            psLoc.setString(11, "O");
                            psLoc.setString(12, recordLO.platform);
                            psLoc.setString(13, "");
                            psLoc.setString(14, recordLO.line);
                            psLoc.setString(15, recordLO.activity);
                            psLoc.setString(16, recordLO.engineeringAllowance);
                            psLoc.setString(17, recordLO.pathingAllowance);
                            psLoc.setString(18, recordLO.performanceAllowance);
                            psLoc.setInt(19, 0);
                            psLoc.addBatch();
                            schedule.add(recordLO);
                            break;
                        }
                        case LI:
                        {
                            CIFLIRecord recordLI = (CIFLIRecord) record;
                            psLoc.setString(1, recordBS.scheduleKey);
                            psLoc.setString(2, recordBS.trainUID);
                            psLoc.setString(3, recordBS.dateRunsFrom);
                            psLoc.setString(4, recordBS.stpIndicator);
                            psLoc.setString(5, recordLI.getLocation());
                            psLoc.setString(6, recordLI.scheduledArrivalTime);
                            psLoc.setString(7, recordLI.scheduledDepartureTime);
                            psLoc.setString(8, recordLI.scheduledPassTime);
                            psLoc.setString(9, recordLI.publicArrivalTime);
                            psLoc.setString(10, recordLI.publicDepartureTime);
                            psLoc.setString(11, "I");
                            psLoc.setString(12, recordLI.platform);
                            psLoc.setString(13, recordLI.path);
                            psLoc.setString(14, recordLI.line);
                            psLoc.setString(15, recordLI.activity);
                            psLoc.setString(16, recordLI.engineeringAllowance);
                            psLoc.setString(17, recordLI.pathingAllowance);
                            psLoc.setString(18, recordLI.performanceAllowance);
                            psLoc.setInt(19, schedule.size());
                            psLoc.addBatch();
                            schedule.add(recordLI);
                            break;
                        }
                        case LT:
                        {
                            CIFLTRecord recordLT = (CIFLTRecord) record;
                            psLoc.setString(1, recordBS.scheduleKey);
                            psLoc.setString(2, recordBS.trainUID);
                            psLoc.setString(3, recordBS.dateRunsFrom);
                            psLoc.setString(4, recordBS.stpIndicator);
                            psLoc.setString(5, recordLT.getLocation());
                            psLoc.setString(6, recordLT.scheduledArrivalTime);
                            psLoc.setString(7, "");
                            psLoc.setString(8, "");
                            psLoc.setString(9, recordLT.publicArrivalTime);
                            psLoc.setString(10, "");
                            psLoc.setString(11, "T");
                            psLoc.setString(12, recordLT.platform);
                            psLoc.setString(13, recordLT.path);
                            psLoc.setString(14, "");
                            psLoc.setString(15, recordLT.activity);
                            psLoc.setString(16, "");
                            psLoc.setString(17, "");
                            psLoc.setString(18, "");
                            psLoc.setInt(19, schedule.size());
                            psLoc.addBatch();
                            schedule.add(recordLT);

                            CIFLORecord recordLO = (CIFLORecord) schedule.get(0);
                            psBS.setBoolean(15, Double.parseDouble(recordLO.scheduledDepartureTime.replace("H", ".5")) >
                                    Double.parseDouble(recordLT.scheduledArrivalTime.replace("H", ".5")));

                            psBS.addBatch();
                            schedule.clear();
                            break;
                        }
                        case CR:
                        {
                            CIFCRRecord recortCR = (CIFCRRecord) record;
                            psCR.setString(1, recordBS.scheduleKey);
                            psCR.setString(2, recordBS.trainUID);
                            psCR.setString(3, recordBS.dateRunsFrom);
                            psCR.setString(4, recordBS.stpIndicator);
                            psCR.setString(5, recortCR.getLocation());
                            psCR.setString(6, recortCR.trainCategory);
                            psCR.setString(7, recortCR.trainIdentity);
                            psCR.setString(8, recortCR.headcode);
                            psCR.setString(9, recortCR.trainServiceCode);
                            psCR.setString(10, recortCR.powerType);
                            psCR.setString(11, recortCR.timingLoad);
                            psCR.setString(12, recortCR.speed);
                            psCR.setString(13, recortCR.operatingChars);
                            psCR.setString(14, recortCR.trainClass);
                            psCR.setString(15, recortCR.sleepers);
                            psCR.setString(16, recortCR.reservations);
                            psCR.setString(17, recortCR.cateringCode);
                            psCR.setString(18, recortCR.serviceBranding);
                            psCR.setInt(19, schedule.size());
                            psCR.addBatch();
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

                if (count % 100000 == 0)
                    needsExec = true;

                if (needsExec && canExec.contains(record.type))
                {
                    needsExec = false;
                    countTI += LongStream.of(psTI.executeLargeBatch())
                            .map(i -> i == Statement.SUCCESS_NO_INFO ? 1 : i).filter(i -> i > 0).sum();
                    countBSDelLocs += LongStream.of(psBSDelLocs.executeLargeBatch())
                            .map(i -> i == Statement.SUCCESS_NO_INFO ? 1 : i).filter(i -> i > 0).sum();
                    countBSDelScheds += LongStream.of(psBSDelScheds.executeLargeBatch())
                            .map(i -> i == Statement.SUCCESS_NO_INFO ? 1 : i).filter(i -> i > 0).sum();
                    countBSDelCR += LongStream.of(psBSDelCR.executeLargeBatch())
                            .map(i -> i == Statement.SUCCESS_NO_INFO ? 1 : i).filter(i -> i > 0).sum();
                    countBSSafeDelLocs += LongStream.of(psBSSafeDelLocs.executeLargeBatch())
                            .map(i -> i == Statement.SUCCESS_NO_INFO ? 1 : i).filter(i -> i > 0).sum();
                    countBSSafeDelScheds += LongStream.of(psBSSafeDelScheds.executeLargeBatch())
                            .map(i -> i == Statement.SUCCESS_NO_INFO ? 1 : i).filter(i -> i > 0).sum();
                    countBSSafeDelCR += LongStream.of(psBSSafeDelCR.executeLargeBatch())
                            .map(i -> i == Statement.SUCCESS_NO_INFO ? 1 : i).filter(i -> i > 0).sum();
                    countBS += LongStream.of(psBS.executeLargeBatch())
                            .map(i -> i == Statement.SUCCESS_NO_INFO ? 1 : i).filter(i -> i > 0).sum();
                    countLoc += LongStream.of(psLoc.executeLargeBatch())
                            .map(i -> i == Statement.SUCCESS_NO_INFO ? 1 : i).filter(i -> i > 0).sum();
                    countCR += LongStream.of(psCR.executeLargeBatch())
                            .map(i -> i == Statement.SUCCESS_NO_INFO ? 1 : i).filter(i -> i > 0).sum();
                }
            }

            try
            {
                countTI += LongStream.of(psTI.executeLargeBatch())
                        .map(i -> i == Statement.SUCCESS_NO_INFO ? 1 : i).filter(i -> i > 0).sum();
                countBSDelLocs += LongStream.of(psBSDelLocs.executeLargeBatch())
                        .map(i -> i == Statement.SUCCESS_NO_INFO ? 1 : i).filter(i -> i > 0).sum();
                countBSDelScheds += LongStream.of(psBSDelScheds.executeLargeBatch())
                        .map(i -> i == Statement.SUCCESS_NO_INFO ? 1 : i).filter(i -> i > 0).sum();
                countBSDelCR += LongStream.of(psBSDelCR.executeLargeBatch())
                        .map(i -> i == Statement.SUCCESS_NO_INFO ? 1 : i).filter(i -> i > 0).sum();
                countBSSafeDelLocs += LongStream.of(psBSSafeDelLocs.executeLargeBatch())
                        .map(i -> i == Statement.SUCCESS_NO_INFO ? 1 : i).filter(i -> i > 0).sum();
                countBSSafeDelScheds += LongStream.of(psBSSafeDelScheds.executeLargeBatch())
                        .map(i -> i == Statement.SUCCESS_NO_INFO ? 1 : i).filter(i -> i > 0).sum();
                countBSSafeDelCR += LongStream.of(psBSSafeDelCR.executeLargeBatch())
                        .map(i -> i == Statement.SUCCESS_NO_INFO ? 1 : i).filter(i -> i > 0).sum();
                countBS += LongStream.of(psBS.executeLargeBatch())
                        .map(i -> i == Statement.SUCCESS_NO_INFO ? 1 : i).filter(i -> i > 0).sum();
                countLoc += LongStream.of(psLoc.executeLargeBatch())
                        .map(i -> i == Statement.SUCCESS_NO_INFO ? 1 : i).filter(i -> i > 0).sum();
                countCR += LongStream.of(psCR.executeLargeBatch())
                        .map(i -> i == Statement.SUCCESS_NO_INFO ? 1 : i).filter(i -> i > 0).sum();

                System.out.printf("TI: %d, BSDelLocs: %d, BSDelScheds: %d, BSDelCR: %d, BSSafeDelLocs: %d, BSSafeDelScheds: %d, BSSafeDelCR: %d, BS: %d, Loc: %d, CR: %d%n",
                countTI, countBSDelLocs, countBSDelScheds, countBSDelCR, countBSSafeDelLocs, countBSSafeDelScheds, countBSSafeDelCR, countBS, countLoc, countCR);

                errcount += countBSSafeDelLocs + countBSSafeDelScheds + countBSSafeDelCR;

                System.out.print("Deleting expired activations... ");
                PreparedStatement psActivations = conn.prepareStatement("DELETE FROM activations WHERE " +
                        "last_update < ((UNIX_TIMESTAMP() - 172800) * 1000) AND last_update != 0");
                PreparedStatement psTRUSTReports = conn.prepareStatement("DELETE trust_reports FROM " +
                        "trust_reports LEFT JOIN activations ON trust_reports.train_id=activations.train_id " +
                        "WHERE activations.train_id is NULL");
                System.out.print(psActivations.executeUpdate());
                System.out.print(" + ");
                System.out.println(psTRUSTReports.executeUpdate());

                //if (config.getBoolean("fullCif"))
                //{
                //    PreparedStatement psTSUpdate = conn.prepareStatement("UPDATE schedules s INNER JOIN (SELECT " +
                //            "schedule_key, creation_timestamp, revision_timestamp FROM stored_timestamps) AS ts ON " +
                //            "s.schedule_key=ts.schedule_key SET s.creation_timestamp=ts.creation_timestamp, " +
                //            "s.revision_timestamp=ts.revision_timestamp;");
                //    PreparedStatement psTSDel = conn.prepareStatement("DELETE FROM stored_timestamps;");

                //    System.out.print("Restoring backed up timestamps...");
                //    psTSUpdate.execute();
                //    psTSDel.execute();
                //    System.out.println();
                //}
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

                System.out.println("Committing...");
                conn.commit();
            }
        }
        catch (IOException e) { e.printStackTrace(); }
        catch (SQLException e)
        {
            System.out.println("Rolling back DB because of:");
            e.printStackTrace();

            try { conn.rollback(); }
            catch (SQLException e2) { e.addSuppressed(e2); }
        }
    }

    private static void gUnzipCIF(File outFile, String cifDownload)
    {
        File inFile = null;
        try
        {
            inFile = downloadData(cifDownload);
            try (GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(inFile));
                 FileOutputStream fos = new FileOutputStream(outFile))
            {
                byte[] buffer = new byte[8192];
                int len;
                while ((len = gzis.read(buffer)) != - 1)
                    fos.write(buffer, 0, len);
            }
        }
        catch (IOException ex)
        {
            System.err.println("Failed to unzip CIF file");
            ex.printStackTrace();

            if (inFile != null && inFile.exists())
                inFile.delete();

            if (outFile != null && outFile.exists())
                outFile.delete();

            System.exit(1);
        }
    }

    private static File downloadData(String type) throws IOException
    {
        File file;
        String url;
        String NROD_URL = "https://" + config.optString("NROD_Static_Location", "datafeeds.networkrail.co.uk");
        if ("corpus".equals(type))
        {
            file = new File(TEMP_DIR, new SimpleDateFormat("yyyy-MM-dd").format(new Date()) + "-CORPUSExtract.json.gz");
            url = NROD_URL + "/ntrod/SupportingFileAuthenticate?type=CORPUS";
        }
        else if ("smart".equals(type))
        {
            file = new File(TEMP_DIR, new SimpleDateFormat("yyyy-MM-dd").format(new Date()) + "-SMARTExtract.json.gz");
            url = NROD_URL + "/ntrod/SupportingFileAuthenticate?type=SMART";
        }
        else if ("cif-full".equals(type))
        {
            file = new File(TEMP_DIR, new SimpleDateFormat("yyyy-MM-dd").format(new Date()) + "-toc-full.CIF.gz");
            url = NROD_URL + "/ntrod/CifFileAuthenticate?type=CIF_ALL_FULL_DAILY&day=toc-full.CIF.gz";
        }
        else if (type.startsWith("cif-update-") && type.length() == 14)
        {
            String day = type.substring(11).toLowerCase();
            if (!Arrays.asList("sun","mon","tue","wed","thu","fri","sat").contains(day))
                throw new IllegalArgumentException(day + " is not a valid day");

            file = new File(TEMP_DIR, new SimpleDateFormat("yyyy-MM-dd").format(new Date()) + "-toc-daily-" + day + ".CIF.gz");
            url = NROD_URL + "/ntrod/CifFileAuthenticate?type=CIF_ALL_UPDATE_DAILY&day=toc-update-" + day + ".CIF.gz";
        }
        else
            throw new IllegalArgumentException("'" + type + "' is not a downloadable file type");

        if (!file.exists())
        {
            HttpsURLConnection con = (HttpsURLConnection) new URL(url).openConnection();
            con.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString((config.getString("NROD_Username") + ":" + config.getString("NROD_Password")).getBytes()));
            con.setInstanceFollowRedirects(false);
            System.out.println("Response 1: " + con.getResponseCode() + " " + con.getResponseMessage());
            if (con.getResponseCode() == HttpsURLConnection.HTTP_MOVED_TEMP || con.getResponseCode() == HttpsURLConnection.HTTP_MOVED_PERM)
            {
                String newLocation = con.getHeaderField("Location");
                System.out.println("Redirected to: " + newLocation);
                con = (HttpsURLConnection) new URL(newLocation).openConnection();
            }

            System.out.println("Response 2: " + con.getResponseCode() + " " + con.getResponseMessage());
            InputStream errIn = con.getErrorStream();
            if (con.getErrorStream() != null)
            {
                Scanner s = new Scanner(errIn).useDelimiter("\\A");
                if (s.hasNext())
                    System.err.println("Error in download: " + s.next());
            }

            if (con.getResponseCode() / 100 == 2)
            {
                InputStream in = con.getInputStream();

                Files.copy(in, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
                in.close();
                System.out.println("File downloaded to " + file.getAbsolutePath());
            }
            else
                System.out.println("Not downloading file due to " + con.getResponseCode() + " error code");
        }
        else
            System.out.println("Using existing file");
        return file;
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
        final String scheduleKey;
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

            scheduleKey = trainUID + dateRunsFrom + stpIndicator + 'C';
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
        private char index;

        CIFLocRecord(CIFRecordType type)
        {
            super(type);
        }

        String getLocation()
        {
            return location;
        }
        char getIndex()
        {
            return index;
        }

        final void setLocation(String location, String index)
        {
            setLocation(location, index.isEmpty() ? ' ' : index.charAt(0));
        }

        final void setLocation(String location, char index)
        {
            this.location = location;
            this.index = index;
        }
    }

    static class CIFLORecord extends CIFLocRecord
    {
      //static final int[] lengths = {2, 7, 1, 5, 4, 3, 3, 2, 2, 12, 2, 37};
        static final int[] offsets = {2, 9, 10, 15, 19, 22, 25, 27, 29, 41, 43, 80};
      //final String location;
      //final String locationIndex;
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
            setLocation(
                    record.substring(offsets[i], offsets[++i]),
                    record.substring(offsets[i], offsets[++i])
            );
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
                                 getLocation(), getIndex(),
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
      //final String location;
      //final String locationIndex;
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
            setLocation(
                    record.substring(offsets[i], offsets[++i]),
                    record.substring(offsets[i], offsets[++i])
            );
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
                                 getLocation(), getIndex(),
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
      //final String location;
      //final String locationIndex;
        final String scheduledArrivalTime;
        final String publicArrivalTime;
        final String platform;
        final String path;
        final String activity;

        CIFLTRecord(String record)
        {
            super(CIFRecordType.LT);

            int i = 0;
            setLocation(
                    record.substring(offsets[i], offsets[++i]),
                    record.substring(offsets[i], offsets[++i])
            );
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
                                 getLocation(), getIndex(),
                                 scheduledArrivalTime, publicArrivalTime,
                                 platform, path, activity);
        }
    }

    static class CIFCRRecord extends CIFLocRecord
    {
      //static final int[] lengths = {2, 7, 1, 2, 4, 4, 1, 8, 1, 3, 4, 3, 6, 1, 1, 1, 1, 4, 4, 4, 5, 8, 5};
        static final int[] offsets = {2, 9, 10, 12, 16, 20, 21, 29, 30, 33, 37, 40, 46, 47, 48, 49, 50, 54, 58, 62, 67, 75, 80};
      //final String location;
      //final String locationIndex;
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
            setLocation(
                    record.substring(offsets[i], offsets[++i]),
                    record.substring(offsets[i], offsets[++i])
            );
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
                                 getLocation(), getIndex(), trainCategory,
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
