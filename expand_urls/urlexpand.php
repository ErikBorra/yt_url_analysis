#!/usr/bin/php5
<?php
/*
 * youtube URL expander
 *
 * Resolves URLs inside youtube descriptions to their final location, if possible.
 *
 * TODO FUTURE IMPROVEMENTS
 *
 * = implement child side caching of identical URLs already resolved
 *
 */

require_once __DIR__ . '/config.php';

// make sure only one URL expander script is running
$thislockfp = script_lock('urlexpand');
if (!is_resource($thislockfp)) {
    logit("urlexpand.log", "urlexpand.php is already running, not starting a second instance");
    exit();
}

global $dbuser, $dbpass, $database, $hostname;

$threads = 16;                    // Number of processes to fork (= number of files to process in parallel)
$timeout = 5;                    // Curl request timeout
$child_sleep_normal = 500;       // Child sleeps n milliseconds after any Curl request
$child_sleep_faster = 200;       // Child sleeps n milliseconds after any Curl request for major websites

$fast_sites = array(
              'j.mp',
              'doubleclick.net',
              'ow.ly',
              'bit.ly',
              'goo.gl',
              'dld.bz',
              'tinyurl.com',
              'fp.me',
              'wp.me',
              'is.gd',
              'twitter.com'
            );


if (!env_is_cli()) {
    die("Please run this script only from the command-line.\n");
}

if (!function_exists('pcntl_fork')) {
    die("Please install and activate the pcntl PHP module to run this script.\n");
}

$urls = array();

logit("urlexpand.log", "starting resolve process");

$child_pids = array();

$files = glob($cache_dir."/toresolve/toresolve_*");

for ($t = 0; $t < $threads; $t++) {

    $pid = pcntl_fork();
    if ($pid == -1) {
        die("thread $t\tCould not fork. You are probably running this script in a restricted hosting environment.\n");
    }
    if ($pid) {
        // We are the parent
        $child_pids[] = $pid;
        logit("urlexpand.log", "thread $t\tforked with pid $pid");
    } else {

        logit("urlexpand.log","thread $t\tChild thread now working");

        // sql stuff
        $dbh = pdo_connect();
        // Increase interactive_timeout. Not sure whether needed, db updates seem to go fast enough
        $prepend_sql = "SET SESSION interactive_timeout = 28800;";
        $rec = $dbh->prepare($prepend_sql);
        $prepend_sql = "SET SESSION wait_timeout = 28800;";
        $rec = $dbh->prepare($prepend_sql);
        $rec->execute();

        // We are the child
        $success = 0;
        $bad = 0;

        // resolve URLS from slice
        $file_name = $files[$t];
        $file = file($file_name);
        logit("urlexpand.log","thread $t\tDoing $file_name");
        exec("mv $file_name $cache_dir/resolved/"); # @todo, bit early here, but trying to avoid stuff being redone when fork crashes or so
        foreach($file as $k => $line) {

            // split file in elements
            $l = explode("\t",$line);
            $url = trim($l[0]);
            $count = trim($l[1]);
            $ids = str_replace("'","",trim($l[2])); 
            $ids = str_replace('"','',$ids);
            $ids = str_replace(", ,",",",$ids);
            $ids = str_replace(",,",",",$ids);
            $ids = preg_replace("/^,/","",$ids);
            $ids = preg_replace("/,$/","",$ids);

            // follow location
            $ch = curl_init();
            curl_setopt($ch, CURLOPT_URL, $url);
            curl_setopt($ch, CURLOPT_USERAGENT, 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36');
            curl_setopt($ch, CURLOPT_VERBOSE, false);
            curl_setopt($ch, CURLOPT_TIMEOUT, $timeout);
            curl_setopt($ch, CURLOPT_HEADER, true);
            curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
            curl_setopt($ch, CURLOPT_NOBODY, true);
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
            curl_setopt($ch, CURLOPT_SSL_VERIFYPEER,false);  //disable ssl certificate validation
	        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST,false);
            $result = curl_exec($ch);
            $error_code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
            curl_close($ch);
            if (preg_match('/.*Location: (http.*?)[\n\r]/ims', $result, $matches)) {
                $resolvedUrl = trim($matches[1]);
            } else if ($error_code == 200) {
                $resolvedUrl = $url;
            } else {
                $resolvedUrl = null;
            }
            if (is_null($resolvedUrl)) {
                // Failed to follow URL
                $sql2 = "UPDATE urls SET dead = 1, status_code = :status_code WHERE id IN (:ids)";
                $rec2 = $dbh->prepare($sql2);
                $rec2->bindParam(":ids", $ids, PDO::PARAM_STR);
                $rec2->bindParam(":status_code", $error_code, PDO::PARAM_INT);
                $rec2->execute();
                $bad++;
                logit("urlexpand.log","BAD\tthread $t\t$file_name\t$k\t$url\t$error_code ");
            } else {
                // Parse host
                $parse = parse_url($resolvedUrl);
                if ($parse !== FALSE) {
                    $sql2 = "UPDATE urls SET resolvedUrl = :resolvedUrl, domain = :domain, resolved=1, status_code = :status_code WHERE id IN ($ids)";
                } else {
                    $sql2 = "UPDATE urls SET resolvedUrl = :resolvedUrl, dead = :dead, resolved=1, status_code = :status_code WHERE id IN ($ids)";
                }
                $rec2 = $dbh->prepare($sql2);
                $rec2->bindParam(":resolvedUrl", $resolvedUrl, PDO::PARAM_STR);
                if ($parse !== FALSE) {
                    $rec2->bindParam(":domain", $parse["host"], PDO::PARAM_STR);
                } else {
                    $rec2->bindParam(":dead", 1, PDO::PARAM_INT);
                }
                $rec2->bindParam(":status_code", $error_code, PDO::PARAM_INT);
                #print $sql2."\n";
                
                if(!preg_match("/^[\d\s,]+$/",$ids)) {
                    logit("urlexpand.log","ERROR SQL\tthread $t\t$file_name\t$k\t$url\t$sql2 ");
                } else
                    $rec2->execute();
                    
                logit("urlexpand.log","OK\tthread $t\t$file_name\t$k\t$url\t$error_code\t$resolvedUrl");
                $success++;
            }
            $parse = parse_url($url);
            if ($parse !== FALSE && in_array($parse["host"], $fast_sites)) {
                usleep($child_sleep_faster);
            } else {
                usleep($child_sleep_normal);
            }
        }
        #if($bad + $success == count($file)) // only move when all urls have been processed
        #exec("mv $file_name $cache_dir/resolved/");
        $str = "thread $t\t$file_name\thandled with " . ($bad + $success) . " updates; $bad bad links and $success successful resolves.";
        logit("urlexpand.log", $str);
        exit(0);
    }
}
logit("urlexpand.log","Main process forked $threads threads; now waiting for completion.");
foreach ($child_pids as $pid) {
    pcntl_waitpid($pid, $status);
}

logit("urlexpand.log","urlexpand.log", "finished for now");
exit(0);

/*
* Acquire a lock as script $script
* If test is true, only test if the lock could be gained, but do not hold on to it (this is how we test if a script is running)
* Returns true on a lock success (in test), false on failure and a lock filepointer if really locking
*/

function script_lock($script, $test = false) {
   $lockfile = __DIR__ . "/proc/$script.lock";

   if (!file_exists($lockfile)) {
       touch($lockfile);
   }
   $lockfp = fopen($lockfile, "r+");

   if (flock($lockfp, LOCK_EX | LOCK_NB)) {  // acquire an exclusive lock
       ftruncate($lockfp, 0);      // truncate file
       fwrite($lockfp, "Locked task '$script' on: " . date("D M d, Y G:i") . "\n");
       fflush($lockfp);            // flush output
       if ($test) {
           flock($lockfp, LOCK_UN);
           fclose($lockfp);
           unlink($lockfile);
           return true;
       }
       return $lockfp;
   } else {
       fclose($lockfp);
       return false;
   }
}

function logit($file, $message) {
   $print_msg = TRUE;
   if(substr($message,0,2) == "OK" || substr($message,0,3)=="BAD")
       $print_msg = FALSE;
   $message = date("Y-m-d H:i:s") . "\t" . $message . "\n";
   if ($file == "cli") {
       print $message;
   } else {
       if($print_msg)
            print $message;
       file_put_contents(__DIR__ . "/logs/" . $file, $message, FILE_APPEND);
   }
}

function env_is_cli() {
    return (!isset($_SERVER['SERVER_SOFTWARE']) && (php_sapi_name() == 'cli' || (is_numeric($_SERVER['argc']) && $_SERVER['argc'] > 0)));
}
function pdo_connect() {
    global $dbuser, $dbpass, $database, $hostname;

    $dbh = new PDO("mysql:host=$hostname;dbname=$database", $dbuser, $dbpass, array(PDO::MYSQL_ATTR_INIT_COMMAND => "set sql_mode='ALLOW_INVALID_DATES'"));
    $dbh->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    $dbh->query("set time_zone='+00:00'");

    return $dbh;
}
?>
