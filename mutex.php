<?php
// Kevin Cheng (18780126619), Chengdu, Sichuan, China
class Mutex {
    protected static $instance;
	private static $lockers = array();
    
	// maximum living duration for a named key;
	private static $max_ttl = 86400;
	
	// segment to decrease time function for locker file
    private static $locker_timer = 43200;
	
	// base dir for all locker files;
	public static $basedir;
	
	// dir for all named keys
	public static $nkdir;
    
	
	public static function get_instance($basedir) {
	    if (null === self::$instance) {
	        self::$instance = new self();
			self::$nkdir = $basedir . '/ttl';
			if (!file_exists (self::$nkdir)){
				if ( @mkdir( self::$nkdir, 0755, true )) {
					// When using the recursive parameter, call chmod() on all created directories;
					@chmod($basedir, 0755);
					@chmod(self::$nkdir, 0755);
				}
			}
			if (!file_exists (self::$nkdir) || !is_readable(self::$nkdir) || !is_writable(self::$nkdir)) {
		        die('Can\'t create \''. self::$nkdir .'\' or permissions to it denied!');
	        } else {
				self::$basedir = $basedir;
			}
			register_shutdown_function(array(self::$instance, 'free_all'));
	    }
		return self::$instance;
    }
	
	public static function set_max_ttl($duration = 86400) {
	    if($duration < 1 || $duration > 86400 * 365 ){
			$duration = 86400;
		}
		self::$max_ttl = $duration;
    }
	
	private static function get_locker($timeout = 5) {
	    return self::wt_add('named_key_locker', $timeout, 1, true);
	}
    
	public static function free_locker($locker) {
	    if(is_resource($locker)){
		    flock( $locker, LOCK_UN );
		    fclose( $locker );
		}
	}
    
	public static function blocker($name, $timeout = 10) {
	    return self::wt_add($name, $timeout);
	}
	
	// set a named key with a living duration ttl(time-to-live) for a specific task;
	public static function set_ttl_key($key, $duration, $force = false) {
		$err = array();
		if(!self::locker_validate($key)){
			$err =  array(
				'code' => 'invalid_key_name',
				'msg'  => 'The name of a key is alphanumeric, underscore or hyphen and no more then 128 characters long.'
			);
		}
		if($duration > self::$max_ttl){
			$err =  array(
				'code' => 'max_ttl',
				'msg'  => 'The time-to-live(TTL) value for a key must be between 0 and the maximum ttl, and no more than 31,536,000(365 days).'
			);
		}
		
		if(empty($err)){
			// get the locker for named key 
			$locker = self::get_locker();
			if(is_resource($locker)){
				// as php function touch is not logically for the future, so set a base past time which won't by pass current time with the pass-in duration;
	            $t = time() - self::$max_ttl;
				$path = self::$nkdir . '/' . $key;
	            $expired = true;
	            if (file_exists($path)){
		            $mt = filemtime($path);
					// not expired, the ttl key is living, meaning taken
		            if (false !== $mt &&  $mt > $t ){
		                $expired = false;
		            } 
	            }
				// named key as file not exists, or expired, or filemtime is false, or if we must force set a ttl for a named key
	            if(($expired || $force) && !touch($path, ($t + $duration))){
		            $err =  array(
				        'code' => 'time_modification',
				        'msg'  => 'Access and modification time of the named key failed!'
			        );
				}
				self::free_locker($locker);
				if(!empty($err)){
					return $err;
				}
				return ($expired || $force);
			} else {
				return $locker;
			}
		}
		return $err;
    }
    
    public static function clear_ttl_key($key) {
	    $locker = self::get_locker();
		if(is_resource($locker)){
			if($handler = opendir(self::$nkdir)) {
		        $t = time() - self::$max_ttl;
		        while (($f = readdir($handler)) !== FALSE) {
		            if ($f != '.' && $f != '..') {
			            $mt = filemtime(self::$nkdir . '/' . $f);
			            if(($f == $key)||(false===$mt)||$mt <= $t){
			                unlink(self::$nkdir . '/' . $f);
			            }
		            }
		        }
		        closedir($handler);
	        }
			self::free_locker($locker);
		}
	}
    
    // $group: task gourp queue
    // $timeout: wait for how long;
    // $length: max concurrent process number for such group. this parameter should have a consistent value across processes, or it makes no sense.
    public static function wt_add($group, $timeout = 10, $length = 1, $named_key_locker = false) {
	    $err = array();
		if(is_null(self::$basedir)){
			$err =  array(
				'code' => 'locker_permission_denied',
				'msg'  => 'The read and write permissions to file lockers\' directory denied.'
			);
		}
	    if($length < 1){
	        $length = 512;
	    }
		
		if($named_key_locker){
	        $base = md5($group);
	    } else {
	        if(self::locker_validate($group)){
				// a base name for a multi-process task with $group and $length integrated.
				$base = $group . '-' . $length;
				// besides a base name, the locker's name must have $current in it for later clean.
				$current = self::mu_time(self::$locker_timer);
				$released = array();
			} else {
		        $err =  array(
				    'code' => 'invalid_locker_name',
				    'msg'  => 'The name of a locker is alphanumeric, underscore or hyphen and no more then 128 characters long.'
			    );
			}
	    }
		if(empty($err)){
			$t = time();
	        $i = 0;
			$gc_mark = '#$#';
			$fp = null;
			
			// loop to get the available non blocking lock, and the loop itself servers as a blocking lock
	        while(true){
	            if($i > ($length - 1)){
		            $i = 0;
	            }
	            if($named_key_locker){
		            // named ttl keys' locker;
					$locker = $base . '.lock';
				} else {
		            // user defined locker
					if(!in_array($i, $released)){
		                // start from the last previous mu_time, as there are processes running which are in the same task group;
						$locker = ($current - 1) . '-' . $base . '-' . $i . '.lock';
					} else {
						$locker = $current . '-' . $base . '-' . $i . '.lock';
		            }
	            }
				// Open the locker file for reading and writing
	            $fp = fopen(self::$basedir .'/'. $locker, 'c+');
	            if ($fp){
		            if (flock($fp, LOCK_EX|LOCK_NB)) {
		                // now we hold the lock
						if($named_key_locker){
							// named key lock
							// stop here and break out
							self::$lockers[] = $fp;
							break;
						} else {
							// check if the locker is for current round;
							if($current . '-' . $base . '-' . $i . '.lock' != $locker){
			                    $gc = '';
			                    if($size = filesize(self::$basedir .'/'. $locker)){
				                    $gc = fread($fp, $size);
			                    }
			                    if($gc_mark == $gc){
				                    $released[] = $i;
								} else {
				                    if($gc){
										// clear old content
										ftruncate( $fp,  0);
										// move the pointer to the beginning
										fseek($fp, 0);
									}
									// append gc_mark
				                    if ( fwrite( $fp, $gc_mark) ) {
				                        $gc = $gc_mark;
				                        ftruncate( $fp, strlen($gc_mark) );
				                        $released[] = $i;
				                    }
				                    fflush( $fp );
			                    }
			                    if($gc_mark == $gc){
				                    // stay at this position for the next round
				                    $i = $i - 1;
			                    }
							} else {
			                    // when loop delay, mu_time might increase;
			                    $mt = self::mu_time(self::$locker_timer);
			                    if($current == $mt){
				                    self::$lockers[] = $fp;
									break;
			                    } else {
				                    // if mu_time increases, do nothing except making the current from mu_time for next round
				                    $current = $mt;
			                    }
							}
		                }
					} 
		            if(is_resource($fp)){
		                flock( $fp, LOCK_UN );
		                fclose( $fp );
		            }
					// check if time out
		            if(time() > $t + $timeout){
		                if($length > 1) {
							$err =  array(
								'code' => 'wait_group_timeout',
				                'msg'  => 'Timeout for wait group!'
			                );
						} else {
							if($named_key_locker){
			                    $err =  array(
								    'code' => 'named_key_timeout',
				                    'msg'  => 'Timeout for named key\'s locker file!'
			                    );
							} else {
			                    $err =  array(
								    'code' => 'blocker_timeout',
				                    'msg'  => 'Timeout for blocker!'
			                    );
							}
		                }
		                break;
		            }
	            } else {
		            if($length > 1) {
		                $err =  array(
							'code' => 'fatal_wait_group',
				            'msg'  => 'Permission denied for the locker files of wait group!'
			            );
					} else {
		                if($named_key_locker){
			                $err =  array(
							    'code' => 'fatal_named_key',
				                'msg'  => 'Permission denied for the locker file of named key!'
			                );
						} else {
			                $err =  array(
							    'code' => 'fatal_blocker',
				                'msg'  => 'Permission denied for the locker files of blocker!'
			                );
						}
		            }
		            break;
	            }
	            $i++;
	        }
		}
		if(empty($err)){
	        return $fp;
	    }
		return $err;
    }
    
	//validate to see if it is to make a valid file name;
    private static function locker_validate($base) {
	    if(preg_match('/^[a-z0-9]*(?:(_|-)[a-z0-9]+)*$/', $base) && strlen($base) < 129 ){
	        return true;
	    }
	    return false;
    }
    
    public static function mu_time($timer = 3600) {
	    if($timer < 3600 || $timer > 15552000){
	        // set a valid value
	        $timer = 3600;
	    }
		return floor(time()/$timer);
    }
    
    public static function trash_lockers(){
	    if(!is_null(self::$basedir) && $handler = opendir(self::$basedir)) {
		    while (false !== ($f = readdir($handler))) {
		        if (is_file($f) && $f != '.' && $f != '..') {
		            $parts = explode('-', $f);
		            if(count($parts) > 1 && (self::mu_time(self::$locker_timer) - intval($parts[0])) > 1){
		                unlink(self::$basedir . '/' . $f);
		            }
		        }
		    }
			closedir($handler);
		}
	}
    
	public static function free_all() {
	    foreach(self::$lockers as $locker){
		    if(is_resource($locker)){
		        flock( $locker, LOCK_UN );
		        fclose( $locker );
		    }
	    }
	}
}
?>
