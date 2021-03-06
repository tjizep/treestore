/// unfortunately mysql has the tendency to redefine system and primitive types like CHAR for instance which changes the
/// interfaces of other libraries include files as to be incompatible with their own source files
/// this neccecitates the inclusion of source files in lieu of the mysql definitions
#ifndef MYSQL_SERVER
	#ifdef _MSC_VER
	#pragma warning(disable:4800)
	#pragma warning(disable:4267)
	#endif
	//#define ENABLED_DEBUG_SYNC
	//#define NDEBUG
	#define DBUG_OFF

	#define MYSQL_DYNAMIC_PLUGIN

	#define MYSQL_SERVER 1

	#ifdef USE_PRAGMA_IMPLEMENTATION
	#pragma implementation                          // gcc: Class implementation
	#endif


	#define MYSQL_SERVER 1

	#ifndef _MSC_VER
	#include <cmath>
	//#define isfinite std::isfinite
	#endif
	#include <algorithm>
	//#include "sql_priv.h"
	#include "probes_mysql.h"
	#include "key.h"                                // key_copy
	#include "sql_plugin.h"
	#include <m_ctype.h>
	#include <my_bit.h>
	#include <stdarg.h>

	#include "sql_table.h"                          // tablename_to_filename
	#include "sql_class.h"                          // THD

	#include <limits>
	#include <map>
	#include <string>
#endif
#include "Poco/Platform.h"

#include "Data/src/Connector.cpp"
#include "Data/src/AbstractBinder.cpp"
#include "Data/src/AbstractBinding.cpp"
#include "Data/src/AbstractExtraction.cpp"
#include "Data/src/AbstractExtractor.cpp"
#include "Data/src/AbstractPreparation.cpp"
#include "Data/src/AbstractPrepare.cpp"
#include "Data/src/BLOB.cpp"
#include "Data/src/BLOBStream.cpp"
#include "Data/src/DataException.cpp"
#include "Data/src/Limit.cpp"
#include "Data/src/MetaColumn.cpp"
#include "Data/src/PooledSessionHolder.cpp"
#include "Data/src/PooledSessionImpl.cpp"
#include "Data/src/Range.cpp"
#include "Data/src/RecordSet.cpp"
#include "Data/src/Session.cpp"
#include "Data/src/SessionImpl.cpp"
#include "Data/src/SessionPool.cpp"
#include "Data/src/SessionFactory.cpp"
#include "Data/src/Statement.cpp"
#include "Data/src/StatementCreator.cpp"
#include "Data/src/StatementImpl.cpp"
#include "Data/SQLite/src/Connector.cpp"
#include "Data/SQLite/src/Binder.cpp"
#include "Data/SQLite/src/Extractor.cpp"
#include "Data/SQLite/src/SessionImpl.cpp"
#include "Data/SQLite/src/SQLiteException.cpp"
#include "Data/SQLite/src/SQLiteStatementImpl.cpp"
#include "Data/SQLite/src/Utility.cpp"

#include "Foundation/src/AbstractObserver.cpp"
#include "Foundation/src/ActiveDispatcher.cpp"
#include "Foundation/src/ArchiveStrategy.cpp"
#include "Foundation/src/Ascii.cpp"
#include "Foundation/src/ASCIIEncoding.cpp"
#include "Foundation/src/AsyncChannel.cpp"
#include "Foundation/src/AtomicCounter.cpp"
#include "Foundation/src/Base64Decoder.cpp"
#include "Foundation/src/Base64Encoder.cpp"
#include "Foundation/src/BinaryReader.cpp"
#include "Foundation/src/BinaryWriter.cpp"
#include "Foundation/src/Bugcheck.cpp"
#include "Foundation/src/ByteOrder.cpp"

#include "Foundation/src/Channel.cpp"
#include "Foundation/src/Checksum.cpp"
#include "Foundation/src/Condition.cpp"
#include "Foundation/src/Configurable.cpp"
#include "Foundation/src/ConsoleChannel.cpp"

#include "Foundation/src/CountingStream.cpp"
#include "Foundation/src/DateTime.cpp"
#include "Foundation/src/DateTimeFormat.cpp"
#include "Foundation/src/DateTimeFormatter.cpp"
#include "Foundation/src/DateTimeParser.cpp"

#include "Foundation/src/Debugger.cpp"
#include "Foundation/src/DeflatingStream.cpp"
#include "Foundation/src/DigestEngine.cpp"
#include "Foundation/src/DigestStream.cpp"
#include "Foundation/src/DirectoryIterator.cpp"
#include "Foundation/src/DirectoryWatcher.cpp"

#include "Foundation/src/DynamicAny.cpp"
#include "Foundation/src/DynamicAnyHolder.cpp"
#include "Foundation/src/ErrorHandler.cpp"
#include "Foundation/src/Event.cpp"
#include "Foundation/src/EventArgs.cpp"
#ifdef POCO_OS_FAMILY_WINDOWS
#include "Foundation/src/EventLogChannel.cpp"
#endif
#include "Foundation/src/Exception.cpp"

#include "Foundation/src/File.cpp"

#include "Foundation/src/FileChannel.cpp"
#include "Foundation/src/FileStream.cpp"
#include "Foundation/src/FileStreamFactory.cpp"
#include "Foundation/src/Format.cpp"
#include "Foundation/src/Formatter.cpp"
#include "Foundation/src/FormattingChannel.cpp"
//#include "Foundation/src/FPEnvironment.cpp"
#include "Foundation/src/Glob.cpp"
#include "Foundation/src/Hash.cpp"
#include "Foundation/src/HashStatistic.cpp"
#include "Foundation/src/HexBinaryDecoder.cpp"
#include "Foundation/src/HexBinaryEncoder.cpp"
#include "Foundation/src/InflatingStream.cpp"
#include "Foundation/src/Latin1Encoding.cpp"
#include "Foundation/src/Latin9Encoding.cpp"
#include "Foundation/src/LineEndingConverter.cpp"
#include "Foundation/src/LocalDateTime.cpp"
#include "Foundation/src/LogFile.cpp"
#include "Foundation/src/Logger.cpp"
#include "Foundation/src/LoggingFactory.cpp"
#include "Foundation/src/LoggingRegistry.cpp"
#include "Foundation/src/LogStream.cpp"
#include "Foundation/src/Manifest.cpp"
//#include "Foundation/src/MD4Engine.cpp"
#include "Foundation/src/MD5Engine.cpp"
#include "Foundation/src/MemoryPool.cpp"
#include "Foundation/src/MemoryStream.cpp"
#include "Foundation/src/Message.cpp"
#include "Foundation/src/Mutex.cpp"
#include "Foundation/src/NamedEvent.cpp"
#ifdef POCO_OS_FAMILY_WINDOWS
#include "Foundation/src/NamedMutex.cpp"
#endif
#include "Foundation/src/NestedDiagnosticContext.cpp"
#include "Foundation/src/Notification.cpp"
#include "Foundation/src/NotificationCenter.cpp"
#include "Foundation/src/NotificationQueue.cpp"
#include "Foundation/src/NullChannel.cpp"
#include "Foundation/src/NullStream.cpp"
#include "Foundation/src/NumberFormatter.cpp"
#include "Foundation/src/NumberParser.cpp"
//#include "Foundation/src/OpcomChannel.cpp"
#include "Foundation/src/Path.cpp"
#include "Foundation/src/PatternFormatter.cpp"
#include "Foundation/src/Pipe.cpp"
#include "Foundation/src/PipeImpl.cpp"
#include "Foundation/src/PipeStream.cpp"
#include "Foundation/src/PriorityNotificationQueue.cpp"
#include "Foundation/src/Process.cpp"
#include "Foundation/src/PurgeStrategy.cpp"
#include "Foundation/src/Random.cpp"
#include "Foundation/src/RandomStream.cpp"
#include "Foundation/src/RefCountedObject.cpp"
//#include "Foundation/src/RegularExpression.cpp"
#include "Foundation/src/RotateStrategy.cpp"
#include "Foundation/src/Runnable.cpp"
#include "Foundation/src/RWLock.cpp"
#include "Foundation/src/Semaphore.cpp"
#include "Foundation/src/SHA1Engine.cpp"
#include "Foundation/src/SharedLibrary.cpp"
#include "Foundation/src/SharedMemory.cpp"
#include "Foundation/src/SignalHandler.cpp"
#include "Foundation/src/SimpleFileChannel.cpp"
#include "Foundation/src/SplitterChannel.cpp"
#include "Foundation/src/Stopwatch.cpp"
#include "Foundation/src/StreamChannel.cpp"
#include "Foundation/src/StreamConverter.cpp"
#include "Foundation/src/StreamCopier.cpp"
#include "Foundation/src/StreamTokenizer.cpp"
#include "Foundation/src/String.cpp"
#include "Foundation/src/StringTokenizer.cpp"
#include "Foundation/src/SynchronizedObject.cpp"
//#include "Foundation/src/SyslogChannel.cpp"
#include "Foundation/src/Task.cpp"
#include "Foundation/src/TaskManager.cpp"
#include "Foundation/src/TaskNotification.cpp"
#include "Foundation/src/TeeStream.cpp"
#include "Foundation/src/TemporaryFile.cpp"
#include "Foundation/src/TextBufferIterator.cpp"
#include "Foundation/src/TextConverter.cpp"
#include "Foundation/src/TextEncoding.cpp"
#include "Foundation/src/TextIterator.cpp"
#include "Foundation/src/Thread.cpp"
#include "Foundation/src/ThreadLocal.cpp"
#include "Foundation/src/ThreadPool.cpp"
#include "Foundation/src/ThreadTarget.cpp"
#include "Foundation/src/TimedNotificationQueue.cpp"
#include "Foundation/src/Timer.cpp"
#include "Foundation/src/Timespan.cpp"
#include "Foundation/src/Timestamp.cpp"
#include "Foundation/src/Timezone.cpp"
#include "Foundation/src/Token.cpp"
#include "Foundation/src/URI.cpp"
#include "Foundation/src/URIStreamFactory.cpp"
#include "Foundation/src/URIStreamOpener.cpp"
#include "Foundation/src/UTF16Encoding.cpp"
#include "Foundation/src/UTF8Encoding.cpp"
#include "Foundation/src/UTF8String.cpp"
#include "Foundation/src/UUID.cpp"
#include "Foundation/src/UUIDGenerator.cpp"
#include "Foundation/src/Void.cpp"
#ifndef _MSC_VER
typedef char BOOL;
#endif
#include "Foundation/src/Unicode.cpp"
#include "Foundation/src/UnicodeConverter.cpp"
#include "Foundation/src/Windows1252Encoding.cpp"
#ifdef POCO_OS_FAMILY_WINDOWS
#include "Foundation/src/WindowsConsoleChannel.cpp"
#endif

#include "Net/src/DNS.cpp"
#include "Net/src/Socket.cpp"
#include "Net/src/SocketImpl.cpp"
#include "Net/src/SocketReactor.cpp"
#include "Net/src/SocketNotification.cpp"
#include "Net/src/SocketNotifier.cpp"
#include "Net/src/RawSocket.cpp"
#include "Net/src/RawSocketImpl.cpp"
#include "Net/src/SocketAddress.cpp"
#include "Net/src/ServerSocket.cpp"
#include "Net/src/ServerSocketImpl.cpp"
#include "Net/src/SocketStream.cpp"
#include "Net/src/StreamSocket.cpp"
#include "Net/src/StreamSocketImpl.cpp"
#include "Net/src/TCPServer.cpp"
#include "Net/src/TCPServerConnection.cpp"
#include "Net/src/TCPServerConnectionFactory.cpp"
#include "Net/src/TCPServerDispatcher.cpp"
#include "Net/src/TCPServerParams.cpp"
#include "Net/src/IPAddress.cpp"
#include "Net/src/NetException.cpp"
#include "Net/src/NetworkInterface.cpp"
#include "Net/src/HostEntry.cpp"
#ifdef _POCO_UTIL_
//#include <string>
//#include "Util/src/AbstractConfiguration.cpp"
#include "Util/src/Application.cpp"
#include "Util/src/ConfigurationMapper.cpp"
#include "Util/src/ConfigurationView.cpp"
#include "Util/src/FilesystemConfiguration.cpp"
#include "Util/src/HelpFormatter.cpp"
#include "Util/src/IniFileConfiguration.cpp"
#include "Util/src/IntValidator.cpp"
#include "Util/src/LayeredConfiguration.cpp"
#include "Util/src/LoggingConfigurator.cpp"
#include "Util/src/LoggingSubsystem.cpp"
#include "Util/src/MapConfiguration.cpp"
#include "Util/src/Option.cpp"
#include "Util/src/OptionCallback.cpp"
#include "Util/src/OptionException.cpp"
#include "Util/src/OptionProcessor.cpp"
#include "Util/src/OptionSet.cpp"
#include "Util/src/PropertyFileConfiguration.cpp"
#include "Util/src/RegExpValidator.cpp"
#include "Util/src/ServerApplication.cpp"
#include "Util/src/Subsystem.cpp"
#include "Util/src/SystemConfiguration.cpp"
#include "Util/src/Timer.cpp"
#include "Util/src/TimerTask.cpp"
#include "Util/src/Validator.cpp"
#ifdef POCO_OS_FAMILY_WINDOWS
//#include "Util/src/WinRegistryConfiguration.cpp"
//#include "Util/src/WinRegistryKey.cpp"
//#include "Util/src/WinService.cpp"
#endif
//#include "Util/src/XMLConfiguration.cpp"
#endif