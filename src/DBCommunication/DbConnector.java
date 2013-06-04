package DBCommunication;

import java.util.ArrayList;

import MailAppUtils.MailAppMessage;



public abstract class DbConnector {
	
	/*
	 * Speichern einer Nachricht in der Datenbank. 
	 * Die Nachricht wird im Inbox-Postfach des Empfängers
	 * und im Outbox-Postfach des Senders gespeichert.
	 */
	public abstract void storeMessage(MailAppMessage message) 
		throws MailAppDBException;
	
	/*
	 * Initialisierung des DBConnectors
	 */
	public abstract void init() throws MailAppDBException;
	
	/*
	 * Überprüfen ob der User existiert und das Passwort korrekt ist.
	 * Liefert true, wenn der User existiert und das Passwort korrekt ist, 
	 * anderenfalls false;
	 */
	public abstract boolean validateUser(String user, String pass)
		throws MailAppDBException;
	
	/*
	 * Abfrage für den POP3 Befehl STAT
	 * Liefert einen String mit dem Muster "<Anzahl Mails> <Größe des Postfachs>"
	 * Beispiel: 3 Mails im Postfach mit Gesamtgröße 2433 Zeichen: "3 2433"
	*/
	public abstract String pop3stat(String user, String mailbox)
		throws MailAppDBException;	
	
	/*
	 * Löschen einer Reihe von Nachrichten aus dem Postfach
	 * Zu löschende Nachrichten werden in einer ArrayList übergeben
	 */
	public abstract void deleteMessages(String user, String mailbox, 
			ArrayList<MailAppMessage> deleteList)throws MailAppDBException;
	
	/*
	 * Abfrage der Nachrichtenanzahl in einem Postfach
	 */
	public abstract int getMessageCount(String user, String mailbox) 
		throws MailAppDBException;
	
	/*
	 * Abfrage einer einzelnen Nachricht
	 */
	public abstract MailAppMessage getMessageByID(String user
			, String mailbox,  String timeUUID) throws MailAppDBException;
	
	/*
	 * Abfrage einer Liste mit IDs und Größen aller Nachrichten eines Postfachs
	 * für UIDL und LIST Befehl
	 */
	public abstract  ArrayList<MailAppMessage> getMessagesUIdList(String user
			, String mailbox) throws MailAppDBException;
}
