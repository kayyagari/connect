package com.mirth.connect.server.controllers.je;

import static com.mirth.connect.donkey.util.SerializerUtil.bytesToInt;
import static com.mirth.connect.donkey.util.SerializerUtil.intToBytes;
import static com.mirth.connect.donkey.util.SerializerUtil.readMessage;
import static com.mirth.connect.donkey.util.SerializerUtil.writeMessageToEntry;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.log4j.Logger;
import org.capnproto.MessageReader;
import org.capnproto.StructList;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormat;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import com.mirth.commons.encryption.Digester;
import com.mirth.connect.client.core.ControllerException;
import com.mirth.connect.donkey.model.message.CapnpModel;
import com.mirth.connect.donkey.model.message.CapnpModel.CapPerson;
import com.mirth.connect.donkey.model.message.CapnpModel.PreferenceEntry;
import com.mirth.connect.donkey.server.BdbJeDataSource;
import com.mirth.connect.donkey.server.Donkey;
import com.mirth.connect.donkey.server.data.jdbc.ReusableMessageBuilder;
import com.mirth.connect.model.Credentials;
import com.mirth.connect.model.LoginStatus;
import com.mirth.connect.model.LoginStatus.Status;
import com.mirth.connect.model.LoginStrike;
import com.mirth.connect.model.PasswordRequirements;
import com.mirth.connect.model.User;
import com.mirth.connect.server.controllers.ControllerFactory;
import com.mirth.connect.server.controllers.ExtensionController;
import com.mirth.connect.server.controllers.UserController;
import com.mirth.connect.server.util.LoginRequirementsChecker;
import com.mirth.connect.server.util.PasswordRequirementsChecker;
import com.mirth.connect.server.util.Pre22PasswordChecker;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Sequence;
import com.sleepycat.je.Transaction;

public class BdbJeUserController extends UserController {

    private Logger logger = Logger.getLogger(this.getClass());
    private ExtensionController extensionController = null;

    private static UserController instance = null;

    private Environment env;
    private Database db;
    private Sequence seq;
    private GenericKeyedObjectPool<Class, ReusableMessageBuilder> serverObjectPool;

    private Digester digester = null;
    private static final Charset utf8 = Charset.forName("utf-8");

    private BdbJeUserController() {
    }

    public static UserController create() {
        synchronized (BdbJeUserController.class) {
            if (instance == null) {
                BdbJeDataSource ds = BdbJeDataSource.getInstance();
                BdbJeUserController i = new BdbJeUserController();
                i.env = ds.getBdbJeEnv();
                String name = "person";
                i.db = ds.getDbMap().get(name);
                i.seq = ds.getServerSeqMap().get(name);
                i.serverObjectPool = ds.getServerObjectPool();
                instance = i;
            }

            return instance;
        }
    }

    @Override
    public void resetUserStatus() {
        Cursor cursor = null;
        Transaction txn = null;
        try {
            txn = env.beginTransaction(null, null);
            cursor = db.openCursor(txn, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS) {
                ReusableMessageBuilder rmb = serverObjectPool.borrowObject(CapPerson.class);
                MessageReader mr = readMessage(data.getData());
                CapPerson.Reader pr = mr.getRoot(CapPerson.factory);
                rmb.getMb().setRoot(CapPerson.factory, pr);
                
                CapPerson.Builder pb = (CapPerson.Builder)rmb.getMb().getRoot(CapPerson.factory);
                pb.setLoggedIn(0);
                writeMessageToEntry(rmb, data);
                db.put(txn, key, data);
                serverObjectPool.returnObject(CapPerson.class, rmb);
            }
            cursor.close();
            txn.commit();
        }
        catch(Exception e) {
            if(cursor != null) {
                cursor.close();
            }
            txn.abort();
            logger.error("couldn't reset login status of all users", e);
        }
    }

    @Override
    public List<User> getAllUsers() throws ControllerException {
        List<User> users = new ArrayList<>();
        Transaction txn = null;
        try {
            txn = env.beginTransaction(null, null);
            Cursor cursor = db.openCursor(txn, null);
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();
            while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS) {
                User u = readUser(data.getData());
                users.add(u);
            }
            cursor.close();
            txn.commit();
        }
        catch(Exception e) {
            txn.abort();
            logger.error("couldn't reset login status of all users", e);
        }
        
        return users;
    }

    @Override
    public User getUser(Integer userId, String userName) throws ControllerException {
        User user = null;
        Transaction txn = null;
        try {
            txn = env.beginTransaction(null, null);
            byte[] data = _getUser(userId, userName, txn);
            if(data != null) {
                user = readUser(data);
            }
            txn.commit();
        }
        catch(Exception e) {
            txn.abort();
            logger.error("couldn't reset login status of all users", e);
        }

        return user;
    }

    @Override
    public void updateUser(User user) throws ControllerException {
        Transaction txn = null;
        ReusableMessageBuilder rmb = null;
        try {
            rmb = serverObjectPool.borrowObject(CapPerson.class);
            txn = env.beginTransaction(null, null);
            byte[] existingUserDataByName = _getUser(null, user.getUsername(), txn);
            //User existingUserByName = getUser(null, user.getUsername());

            if (user.getId() == null) {
                if (existingUserDataByName != null) {
                    throw new ControllerException("username must be unique");
                }

                logger.debug("adding user " + user);
                CapPerson.Builder pb = (CapPerson.Builder) rmb.getSb();
                toCapUser(user, pb);
                pb.setGracePeriodStart(-1);
                pb.setLastLogin(-1);
                pb.setLastStrikeTime(-1);
                pb.setLoggedIn(0);
                pb.setPasswordDate(-1);
                pb.setStrikeCount(0);
                
                int userId = (int)seq.get(txn, 1);
                pb.setId(userId);
                user.setId(userId);
                DatabaseEntry key = new DatabaseEntry(buildPk(userId, user.getUsername()));
                DatabaseEntry data = new DatabaseEntry();
                writeMessageToEntry(rmb, data);
                db.put(txn, key, data);
            } else {
                if(existingUserDataByName != null) {
                    User existingUserByName = readUser(existingUserDataByName);
                    if (!user.getId().equals(existingUserByName.getId())) {
                        throw new ControllerException("Error updating user, username must be unique");
                    }
                }

                byte[] existingUserDataById = _getUser(user.getId(), null, txn);
                if (existingUserDataById == null) {
                    throw new ControllerException("Error updating user, No user found with ID " + user.getId());
                }
                
                MessageReader mr = readMessage(existingUserDataById);
                CapPerson.Reader pr = mr.getRoot(CapPerson.factory);
                rmb.reset();
                rmb.getMb().setRoot(CapPerson.factory, pr);
                CapPerson.Builder pb = (CapPerson.Builder) rmb.getMb().getRoot(CapPerson.factory);

                String currentUsername = pb.getUsername().toString();

                logger.debug("updating user " + user);
                toCapUser(user, pb);
                DatabaseEntry key = new DatabaseEntry(buildPk(user.getId(), user.getUsername()));
                DatabaseEntry data = new DatabaseEntry();
                writeMessageToEntry(rmb, data);
                db.put(txn, key, data);
                // Notify the authorization controller if the username changed
                if (!StringUtils.equals(currentUsername, user.getUsername())) {
                    ControllerFactory.getFactory().createAuthorizationController().usernameChanged(currentUsername, user.getUsername());
                }
            }
            
            txn.commit();
        } catch (Exception e) {
            if(txn != null) {
                txn.abort();
            }
            throw new ControllerException(e);
        } finally {
            if(rmb != null) {
                serverObjectPool.returnObject(CapPerson.class, rmb);
            }
        }        
    }

    @Override
    public List<String> checkOrUpdateUserPassword(Integer userId, String plainPassword) throws ControllerException {
        Transaction txn = null;
        ReusableMessageBuilder rmb = null;
        try {
            if(digester == null) {
                digester = ControllerFactory.getFactory().createConfigurationController().getDigester();
            }
            PasswordRequirements passwordRequirements = ControllerFactory.getFactory().createConfigurationController().getPasswordRequirements();
            // unlike in RDBMS password history is not stored in this version, so passing userId as null
            List<String> responses = PasswordRequirementsChecker.getInstance().doesPasswordMeetRequirements(null, plainPassword, passwordRequirements);
            if (responses != null) {
                return responses;
            }

            /*
             * If no userId was passed in, stop here and don't try to add the password.
             */
            if (userId == null) {
                return null;
            }

            logger.debug("updating password for user id " + userId);

            rmb = serverObjectPool.borrowObject(CapPerson.class);
            txn = env.beginTransaction(null, null);
            byte[] userData = _getUser(userId, null, txn);
            MessageReader mr = readMessage(userData);
            CapPerson.Reader pr = mr.getRoot(CapPerson.factory);
            rmb.getMb().setRoot(CapPerson.factory, pr);
            CapPerson.Builder pb = (CapPerson.Builder) rmb.getMb().getRoot(CapPerson.factory);

            pb.setPassword(digester.digest(plainPassword));
            pb.setPasswordDate(System.currentTimeMillis());
            pb.setGracePeriodStart(-1);

            DatabaseEntry key = new DatabaseEntry(buildPk(userId, pb.getUsername().toString()));
            DatabaseEntry data = new DatabaseEntry();
            writeMessageToEntry(rmb, data);
            db.put(txn, key, data);
            txn.commit();

            return null;
        } catch (Exception e) {
            if(txn != null) {
                txn.abort();
            }
            throw new ControllerException(e);
        } finally {
            if(rmb != null) {
                serverObjectPool.returnObject(CapPerson.class, rmb);
            }
        }
    }

    @Override
    public void removeUser(Integer userId, Integer currentUserId) throws ControllerException {
        logger.debug("removing user " + userId);

        if (userId == null) {
            throw new ControllerException("Error removing user, User Id cannot be null");
        }

        if (userId.equals(currentUserId)) {
            throw new ControllerException("Error removing user, You cannot remove yourself");
        }

        Transaction txn = null;
        try {
            txn = env.beginTransaction(null, null);
            byte[] userKey = _getUserKeyOrData(userId, null, txn, true);
            if(userKey != null) {
                db.delete(txn, new DatabaseEntry(userKey));
            }
            txn.commit();
        } catch (PersistenceException e) {
            if(txn != null) {
                txn.abort();
            }
            throw new ControllerException(e);
        }
    }

    @Override
    public LoginStatus authorizeUser(String username, String plainPassword) throws ControllerException {
        Transaction txn = null;
        ReusableMessageBuilder rmb = null;
        try {
            // Invoke and return from the Authorization Plugin if one exists
            if (extensionController == null) {
                extensionController = ControllerFactory.getFactory().createExtensionController();
            }

            if (extensionController.getAuthorizationPlugin() != null) {
                LoginStatus loginStatus = extensionController.getAuthorizationPlugin().authorizeUser(username, plainPassword);

                /*
                 * A null return value indicates that the authorization plugin is disabled or is
                 * otherwise delegating control back to the UserController to perform
                 * authentication.
                 */
                if (loginStatus != null) {
                    return handleSecondaryAuthentication(username, loginStatus, null);
                }
            }

            boolean authorized = false;
            String credentials = null;
            PasswordRequirements passwordRequirements = ControllerFactory.getFactory().createConfigurationController().getPasswordRequirements();

            // Retrieve the matching User
            txn = env.beginTransaction(null, null);
            byte[] userData = _getUser(null, username, txn);
            CapPerson.Builder pb = null;
            if (userData != null) {
                rmb = serverObjectPool.borrowObject(CapPerson.class);
                MessageReader mr = readMessage(userData);
                CapPerson.Reader pr = mr.getRoot(CapPerson.factory);
                rmb.getMb().setRoot(CapPerson.factory, pr);
                pb = (CapPerson.Builder) rmb.getMb().getRoot(CapPerson.factory);

                if(digester == null) {
                    digester = ControllerFactory.getFactory().createConfigurationController().getDigester();
                }
                
                boolean lockoutEnabled = (passwordRequirements.getRetryLimit() > 0);
                if (lockoutEnabled) {
                    int retryLimit = passwordRequirements.getRetryLimit();
                    retryLimit = retryLimit + 1 - (pb.getStrikeCount() != -1 ? pb.getStrikeCount() : 0);
                    
                    Duration lockoutPeriod = Duration.standardHours(passwordRequirements.getLockoutPeriod());
                    long lastStrikeTime = pb.getLastStrikeTime() != -1 ? pb.getLastStrikeTime() : 0;
                    Duration strikeDuration = new Duration(lastStrikeTime, System.currentTimeMillis());
                    long strikeTimeRemaining = lockoutPeriod.minus(strikeDuration).getMillis();

                    if(retryLimit <= 0 && strikeTimeRemaining > 0) {
                        return new LoginStatus(LoginStatus.Status.FAIL_LOCKED_OUT, "User account \"" + username + "\" has been locked. You may attempt to login again in " + getPrintableStrikeTimeRemaining(strikeTimeRemaining) + ".");
                    }
                }
                
                // Validate the user credentials
                credentials = pb.getPassword().toString();
                
                if (credentials != null) {
                    if (Pre22PasswordChecker.isPre22Hash(credentials)) {
                        if (Pre22PasswordChecker.checkPassword(plainPassword, credentials)) {
                            authorized = true;
                        }
                    } else {
                        authorized = digester.matches(plainPassword, credentials);
                    }
                }
            }

            LoginStatus loginStatus = null;

            if (authorized) {
                // If password expiration is enabled, do checks now
                if (passwordRequirements.getExpiration() > 0) {
                    long passwordTime = pb.getPasswordDate();
                    long currentTime = System.currentTimeMillis();

                    Duration expirationDuration = Duration.standardDays(passwordRequirements.getExpiration());
                    Duration passwordDuration = new Duration(passwordTime, currentTime);

                    boolean passwordExpired = (expirationDuration.minus(passwordDuration).getMillis() < 0);

                    // If the password is expired, do grace period checks
                    if (passwordExpired) {
                        // Let 0 be infinite grace period, -1 be no grace period
                        if (passwordRequirements.getGracePeriod() == 0) {
                            loginStatus = new LoginStatus(LoginStatus.Status.SUCCESS_GRACE_PERIOD, "Your password has expired. Please change your password now.");
                        } else if (passwordRequirements.getGracePeriod() > 0) {
                            // If there has never been a grace time, start it now
                            long gracePeriodStartTime;
                            if (pb.getGracePeriodStart() == -1) {
                                gracePeriodStartTime = currentTime;
                                pb.setGracePeriodStart(gracePeriodStartTime);
                            } else {
                                gracePeriodStartTime = pb.getGracePeriodStart();
                            }

                            Duration gracePeriodExpirationDuration = Duration.standardDays(passwordRequirements.getGracePeriod());
                            Duration gracePeriodDuration = new Duration(gracePeriodStartTime, currentTime);

                            long graceTimeRemaining = gracePeriodExpirationDuration.minus(gracePeriodDuration).getMillis();
                            if (graceTimeRemaining > 0) {
                                loginStatus = new LoginStatus(LoginStatus.Status.SUCCESS_GRACE_PERIOD, "Your password has expired. You are required to change your password in the next " + getPrintableGraceTimeRemaining(graceTimeRemaining) + ".");
                            }
                        }

                        // If there is no grace period or it has passed, FAIL_EXPIRED
                        if (loginStatus == null) {
                            loginStatus = new LoginStatus(LoginStatus.Status.FAIL_EXPIRED, "Your password has expired. Please contact an administrator to have your password reset.");
                        }

                        /*
                         * Reset the user's grace period if it isn't being used but one was
                         * previously set. This should only happen if a user is in a grace period
                         * before grace periods are disabled.
                         */
                        if ((passwordRequirements.getGracePeriod() <= 0) && (pb.getGracePeriodStart() != -1)) {
                            pb.setGracePeriodStart(-1);
                        }
                    }
                }
                // End of password expiration and grace period checks

                // If nothing failed (loginStatus != null), set SUCCESS now
                if (loginStatus == null) {
                    loginStatus = new LoginStatus(LoginStatus.Status.SUCCESS, "");

                    // Clear the user's grace period if one exists
                    if (pb.getGracePeriodStart() != -1) {
                        pb.setGracePeriodStart(-1);
                    }
                }
                
                DatabaseEntry key = new DatabaseEntry(buildPk(pb.getId(), pb.getUsername().toString()));
                DatabaseEntry data = new DatabaseEntry();
                writeMessageToEntry(rmb, data);
                db.put(txn, key, data);
            } else {
                LoginStatus.Status status = LoginStatus.Status.FAIL;
                String failMessage = "Incorrect username or password.";

                if(pb != null) {
                    pb.setStrikeCount(pb.getStrikeCount() + 1);
                    
                    if (passwordRequirements.getRetryLimit() > 0) {
                        int retryLimit = passwordRequirements.getRetryLimit();
                        int attemptsRemaining = (retryLimit + 1 - pb.getStrikeCount());
                        
                        Duration lockoutPeriod = Duration.standardHours(passwordRequirements.getLockoutPeriod());
                        long lastStrikeTime = System.currentTimeMillis();
                        long strikeTimeRemaining = lockoutPeriod.minus(lastStrikeTime).getMillis();
                        
                        if (attemptsRemaining <= 0 && strikeTimeRemaining > 0) {
                            status = LoginStatus.Status.FAIL_LOCKED_OUT;
                            failMessage += " User account \"" + username + "\" has been locked. You may attempt to login again in " + getPrintableStrikeTimeRemaining(strikeTimeRemaining) + ".";
                        } else {
                            String printableLockoutPeriod = PeriodFormat.getDefault().print(Period.hours(passwordRequirements.getLockoutPeriod()));
                            failMessage += " " + attemptsRemaining + " login attempt(s) remaining for \"" + username + "\" until the account is locked for " + printableLockoutPeriod + ".";
                        }
                    }
                }

                loginStatus = new LoginStatus(status, failMessage);
            }

            txn.commit();
            return handleSecondaryAuthentication(username, loginStatus, null);
        } catch (Exception e) {
            if(txn != null) {
                txn.abort();
            }
            throw new ControllerException(e);
        }
        finally {
            if(rmb != null) {
                serverObjectPool.returnObject(CapPerson.class, rmb);
            }
        }
    }

    @Override
    public boolean checkPassword(String plainPassword, String encryptedPassword) {
        if(digester == null) {
            digester = ControllerFactory.getFactory().createConfigurationController().getDigester();
        }
        return digester.matches(plainPassword, encryptedPassword);
    }

    @Override
    public void loginUser(User user) throws ControllerException {
        _setLoginStatus(user, true);
    }

    @Override
    public void logoutUser(User user) throws ControllerException {
        _setLoginStatus(user, false);
    }

    private void _setLoginStatus(User user, boolean login) throws ControllerException {
        Transaction txn = null;
        ReusableMessageBuilder rmb = null;
        try {
            txn = env.beginTransaction(null, null);
            byte[] data = _getUser(user.getId(), user.getUsername(), txn);
            if(data != null) {
                CapPerson.Reader pr = readMessage(data).getRoot(CapPerson.factory);
                rmb = serverObjectPool.borrowObject(CapPerson.class);
                rmb.getMb().setRoot(CapPerson.factory, pr);
                CapPerson.Builder pb = (CapPerson.Builder)rmb.getMb().getRoot(CapPerson.factory);
                if(login) {
                    pb.setLoggedIn(1);
                    pb.setLastLogin(System.currentTimeMillis());
                }
                else {
                    pb.setLoggedIn(0);
                }
                
                DatabaseEntry key = new DatabaseEntry(buildPk(pb.getId(), pb.getUsername().toString()));
                DatabaseEntry val = new DatabaseEntry();
                writeMessageToEntry(rmb, val);
                db.put(txn, key, val);
            }
            txn.commit();
        }
        catch(Exception e) {
            if(txn != null) {
                txn.abort();
            }
            throw new ControllerException(e);
        }
        finally {
            if(rmb != null) {
                serverObjectPool.returnObject(CapPerson.class, rmb);
            }
        }
    }

    @Override
    public boolean isUserLoggedIn(Integer userId) throws ControllerException {
        Function<CapPerson.Reader, Boolean> f = new Function<CapPerson.Reader, Boolean>() {
            @Override
            public Boolean apply(CapPerson.Reader pr) {
                if(pr == null) {
                    return false;
                }
                return pr.getLoggedIn() == 1;
            }
        };

        return execReadFunction(userId, f);
    }

    @Override
    public List<Credentials> getUserCredentials(Integer userId) throws ControllerException {
        Function<CapPerson.Reader, List<Credentials>> f = new Function<CapPerson.Reader, List<Credentials>>() {
            @Override
            public List<Credentials> apply(CapPerson.Reader pr) {
                List<Credentials> lst = new ArrayList<>();
                if(pr != null && pr.hasPassword()) {
                    Credentials cred = new Credentials();
                    cred.setPassword(pr.getPassword().toString());
                    Calendar c = Calendar.getInstance();
                    c.setTimeInMillis(pr.getPasswordDate());
                    cred.setPasswordDate(c);
                    lst.add(cred);
                }

                return lst;
            }
        };

        return execReadFunction(userId, f);
    }

    @Override
    public LoginStrike incrementStrikes(Integer userId) throws ControllerException {
        Function<CapPerson.Builder, LoginStrike> f = new Function<CapPerson.Builder, LoginStrike>() {
            @Override
            public LoginStrike apply(CapPerson.Builder pb) {
                if(pb == null) {
                    return null;
                }
                int lastStrikeCount = pb.getStrikeCount();
                long lastTime = pb.getLastStrikeTime();
                Calendar lastStrikeTime = null;
                if(lastTime != -1) {
                    lastStrikeTime = Calendar.getInstance();
                    lastStrikeTime.setTimeInMillis(lastTime);
                }
                LoginStrike ls = new LoginStrike(lastStrikeCount, lastStrikeTime);
                pb.setStrikeCount(lastStrikeCount + 1);
                return ls;
            }
        };
        return execWriteFunction(userId, f);
    }

    @Override
    public LoginStrike resetStrikes(Integer userId) throws ControllerException {
        Function<CapPerson.Builder, LoginStrike> f = new Function<CapPerson.Builder, LoginStrike>() {
            @Override
            public LoginStrike apply(CapPerson.Builder pb) {
                if(pb == null) {
                    return null;
                }
                pb.setLastStrikeTime(-1);
                pb.setStrikeCount(0);
                
                LoginStrike ls = new LoginStrike(0, null);
                return ls;
            }
        };
        return execWriteFunction(userId, f);
    }

    @Override
    public String getUserPreference(Integer userId, String name) throws ControllerException {
        Function<CapPerson.Reader, String> f = new Function<CapPerson.Reader, String>() {
            @Override
            public String apply(CapPerson.Reader pr) {
                if(pr == null || !pr.hasPreferences()) {
                    return null;
                }
                
                StructList.Reader<CapnpModel.PreferenceEntry.Reader> r = pr.getPreferences();
                Iterator<CapnpModel.PreferenceEntry.Reader> iter = r.iterator();
                String val = null;
                while(iter.hasNext()) {
                    CapnpModel.PreferenceEntry.Reader entryReader = iter.next();
                    if(name.equals(entryReader.getKey().toString())) {
                        Object tmp = entryReader.getValue();
                        if(tmp != null) {
                            val = String.valueOf(tmp);
                        }
                        break;
                    }
                }
                return val;
            }
        };
        
        return execReadFunction(userId, f);
    }

    @Override
    public Properties getUserPreferences(Integer userId, Set<String> names) throws ControllerException {
        Function<CapPerson.Reader, Properties> f = new Function<CapPerson.Reader, Properties>() {
            @Override
            public Properties apply(CapPerson.Reader pr) {
                Properties props = new Properties();

                if(pr != null && pr.hasPreferences()) {
                    StructList.Reader<CapnpModel.PreferenceEntry.Reader> r = pr.getPreferences();
                    Iterator<CapnpModel.PreferenceEntry.Reader> iter = r.iterator();
                    while(iter.hasNext()) {
                        CapnpModel.PreferenceEntry.Reader entryReader = iter.next();
                        String val = entryReader.getValue().toString();
                        String key = entryReader.getKey().toString();
                        props.put(key, val);
                    }
                }

                return props;
            }
        };

        return execReadFunction(userId, f);
    }

    @Override
    public void setUserPreference(Integer userId, String name, String value) throws ControllerException {
        Function<CapPerson.Builder, Void> f = new Function<CapPerson.Builder, Void>() {
            @Override
            public Void apply(CapPerson.Builder pb) {
                Properties props = new Properties();
                StructList.Builder<PreferenceEntry.Builder> lstBuilder = null;
                int nextIndex = -1;
                if(pb.hasPreferences()) {
                    lstBuilder = pb.getPreferences();
                    for(int i=0; i<lstBuilder.size(); i++) {
                        PreferenceEntry.Builder b = lstBuilder.get(i);
                        props.put(b.getKey().toString(), b.getValue().toString());
                    }
                    nextIndex = props.size() - 1;
                }
                else {
                    lstBuilder = pb.initPreferences(1);
                    nextIndex = 0;
                }

                PreferenceEntry.Builder b = lstBuilder.get(nextIndex);
                b.setKey(name);
                b.setValue(value);
                return null;
            }
        };
        
        execWriteFunction(userId, f);
    }

    @Override
    public void setUserPreferences(Integer userId, Properties properties) throws ControllerException {
        Function<CapPerson.Builder, Void> f = new Function<CapPerson.Builder, Void>() {
            @Override
            public Void apply(CapPerson.Builder pb) {
                StructList.Builder<PreferenceEntry.Builder> lstBuilder = pb.initPreferences(properties.size());
                int i = 0;
                for(Map.Entry<Object, Object> e : properties.entrySet()) {
                    PreferenceEntry.Builder b = lstBuilder.get(i);
                    b.setKey(String.valueOf(e.getKey()));
                    String val = "";
                    if(e.getValue() != null) {
                       val = String.valueOf(e.getValue());
                    }
                    b.setValue(val);
                }
                return null;
            }
        };
        execWriteFunction(userId, f);
    }

    @Override
    public void removePreferencesForUser(int id) {
        Function<CapPerson.Builder, Void> f = new Function<CapPerson.Builder, Void>() {
            @Override
            public Void apply(CapPerson.Builder pb) {
                pb.initPreferences(0);
                return null;
            }
        };
        try {
            execWriteFunction(id, f);
        }
        catch(ControllerException e) {
            logger.warn("could not remove preferences of the user " + id, e);
        }
    }

    @Override
    public void removePreference(int id, String name) {
    }

    private User readUser(byte[] data) throws Exception {
        MessageReader mr = readMessage(data);
        CapPerson.Reader pr = mr.getRoot(CapPerson.factory);
        User u = new User();
        if(pr.hasDescription()) {
            u.setDescription(pr.getDescription().toString());
        }
        if(pr.hasEmail()) {
            u.setEmail(pr.getEmail().toString());
        }
        
        if(pr.hasFirstname()) {
            u.setFirstName(pr.getFirstname().toString());
        }

        long gracePeriodStart = pr.getGracePeriodStart();
        if(gracePeriodStart != -1) {
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(gracePeriodStart);
            u.setGracePeriodStart(c);
        }
        
        u.setId(pr.getId());
        if(pr.hasIndustry()) {
            u.setIndustry(pr.getIndustry().toString());
        }
        
        long lastLogin = pr.getLastLogin();
        if(lastLogin != -1) {
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(lastLogin);
            u.setLastLogin(c);
        }
        
        if(pr.hasLastname()) {
            u.setLastName(pr.getLastname().toString());
        }
        
        long lastStrike = pr.getLastStrikeTime();
        if(lastStrike != -1) {
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(lastStrike);
            u.setLastStrikeTime(c);
        }
        
        if(pr.hasOrganization()) {
            u.setOrganization(pr.getOrganization().toString());
        }
        
        if(pr.hasPhonenumber()) {
            u.setPhoneNumber(pr.getPhonenumber().toString());
        }
        
        u.setStrikeCount(pr.getStrikeCount());
        u.setUsername(pr.getUsername().toString());
        
        return u;
    }
    
    private byte[] _getUser(Integer userId, String userName, Transaction txn) {
        return _getUserKeyOrData(userId, userName, txn, false);
    }

    private byte[] _getUserKeyOrData(Integer userId, String userName, Transaction txn, boolean returnKey) {
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        boolean found = false;

        if(userName != null) {
            userName = userName.toLowerCase();
        }

        if(userId != null && userName != null) {
            key.setData(buildPk(userId, userName));
            OperationStatus os = db.get(txn, key, data, LockMode.READ_COMMITTED);
            if(os == OperationStatus.SUCCESS) {
                found = true;
            }
        }
        else {
            Cursor cursor = db.openCursor(txn, null);
            try {
                while(cursor.getNext(key, data, null) == OperationStatus.SUCCESS) {
                    if(userId != null && userName == null) {
                        int i = bytesToInt(key.getData());
                        if(userId == i) {
                            found = true;
                        }
                    }
                    else {
                        byte[] keyBytes = key.getData();
                        String s = new String(keyBytes, 4, keyBytes.length - 4);
                        if(s.equals(userName)) {
                            found = true;
                        }
                    }
                    if(found) {
                        break;
                    }
                }
            }
            finally {
                cursor.close();
            }
        }
        
        if(found) {
            if(returnKey) {
                return key.getData();
            }

            return data.getData();
        }

        return null;
    }

    private byte[] buildPk(int userId, String username) {
        byte[] tmp = username.toLowerCase().getBytes(utf8);
        byte[] key = new byte[4 + tmp.length];
        intToBytes(userId, key, 0);
        System.arraycopy(tmp, 0, key, 4, tmp.length);
        
        return key;
    }

    private void toCapUser(User user, CapPerson.Builder pb) {
        if (user.getId() != null) {
            pb.setId(user.getId());
        }

        pb.setUsername(user.getUsername().toLowerCase());
        if(user.getFirstName() != null) {
            pb.setFirstname(user.getFirstName());
        }
        if(user.getLastName() != null) {
            pb.setLastname(user.getLastName());
        }
        if(user.getOrganization() != null) {
            pb.setOrganization(user.getOrganization());
        }
        if(user.getIndustry() != null) {
            pb.setIndustry(user.getIndustry());
        }
        if(user.getEmail() != null) {
            pb.setEmail(user.getEmail());
        }
        if(user.getPhoneNumber() != null) {
            pb.setPhonenumber(user.getPhoneNumber());
        }
        if(user.getDescription() != null) {
            pb.setDescription(user.getDescription());
        }
    }

    private <R> R execReadFunction(Integer userId, Function<CapPerson.Reader, R> f) throws ControllerException {
        Transaction txn = null;
        try {
            txn = env.beginTransaction(null, null);
            byte[] data = _getUser(userId, null, txn);
            CapPerson.Reader pr = null;
            if(data != null) {
                pr = readMessage(data).getRoot(CapPerson.factory);
            }
            txn.commit();
            return f.apply(pr);
        }
        catch(Exception e) {
            if(txn != null) {
                txn.abort();
            }
            throw new ControllerException(e);
        }
    }

    private <R> R execWriteFunction(Integer userId, Function<CapPerson.Builder, R> f) throws ControllerException {
        Transaction txn = null;
        ReusableMessageBuilder rmb = null;
        try {
            R result = null;
            rmb = serverObjectPool.borrowObject(CapPerson.class);
            txn = env.beginTransaction(null, null);
            byte[] data = _getUser(userId, null, txn);
            CapPerson.Builder pb = null;
            String username = null;
            if(data != null) {
                CapPerson.Reader pr = readMessage(data).getRoot(CapPerson.factory);
                rmb.getMb().setRoot(CapPerson.factory, pr);
                pb = (CapPerson.Builder) rmb.getMb().getRoot(CapPerson.factory);
                username = pb.getUsername().toString();
            }
            result = f.apply(pb);
            if(username != null) {
                DatabaseEntry key = new DatabaseEntry(buildPk(userId, username));
                DatabaseEntry val = new DatabaseEntry();
                writeMessageToEntry(rmb, val);
                db.put(txn, key, val);
            }
            txn.commit();
            return result;
        }
        catch(Exception e) {
            if(txn != null) {
                txn.abort();
            }
            throw new ControllerException(e);
        }
        finally {
            if(rmb != null) {
                serverObjectPool.returnObject(CapPerson.class, rmb);
            }
        }
    }

    private LoginStatus handleSecondaryAuthentication(String username, LoginStatus loginStatus, LoginRequirementsChecker loginRequirementsChecker) {
        if (loginStatus != null && extensionController.getMultiFactorAuthenticationPlugin() != null && (loginStatus.getStatus() == Status.SUCCESS || loginStatus.getStatus() == Status.SUCCESS_GRACE_PERIOD)) {
            loginStatus = extensionController.getMultiFactorAuthenticationPlugin().authenticate(username, loginStatus);
        }

        // Only reset strikes if the final status is successful 
        if (loginStatus.getStatus() == Status.SUCCESS || loginStatus.getStatus() == Status.SUCCESS_GRACE_PERIOD) {
            try {
                User user = getUser(null, username);
                resetStrikes(user.getId());
            } catch (ControllerException e) {
                logger.warn("Unable to reset strikes for user \"" + username + "\": Could not find user.", e);
            }
        }

        return loginStatus;
    }

    private String getPrintableGraceTimeRemaining(long graceTimeRemaining) {
        Period period = new Period(graceTimeRemaining);

        PeriodFormatter periodFormatter;
        if (period.toStandardHours().getHours() > 0) {
            periodFormatter = new PeriodFormatterBuilder().printZeroRarelyFirst().appendDays().appendSuffix(" day", " days").appendSeparator(" and ").printZeroAlways().appendHours().appendSuffix(" hour", " hours").toFormatter();
        } else {
            periodFormatter = new PeriodFormatterBuilder().printZeroNever().appendMinutes().appendSuffix(" minute", " minutes").appendSeparator(" and ").printZeroAlways().appendSeconds().appendSuffix(" second", " seconds").toFormatter();
        }

        return periodFormatter.print(period);
    }

    private String getPrintableStrikeTimeRemaining(long strikeTimeRemaining) {
        Period period = new Period(strikeTimeRemaining);

        PeriodFormatter periodFormatter;
        if (period.toStandardMinutes().getMinutes() > 0) {
            periodFormatter = new PeriodFormatterBuilder().printZeroNever().appendHours().appendSuffix(" hour", " hours").appendSeparator(" and ").printZeroAlways().appendMinutes().appendSuffix(" minute", " minutes").toFormatter();
        } else {
            periodFormatter = new PeriodFormatterBuilder().printZeroAlways().appendSeconds().appendSuffix(" second", " seconds").toFormatter();
        }

        return periodFormatter.print(period);
    }
    
    // for use in tests
    public void _removeAllUsers() {
        Transaction txn = env.beginTransaction(null, null);
        // should not call env.truncate
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry data = new DatabaseEntry();
        Cursor c = db.openCursor(txn, null);
        while(c.getNext(key, data, null) == OperationStatus.SUCCESS) {
            c.delete();
        }
        c.close();
        txn.commit();
    }
    
    public void createDefaultUser() throws ControllerException {
        String username = "admin";
        User admin = getUser(null, username);
        if(admin == null) {
            logger.info("creating default user account");
            admin = new User();
            admin.setUsername(username);
            updateUser(admin);
            checkOrUpdateUserPassword(admin.getId(), username);
        }
    }
}
