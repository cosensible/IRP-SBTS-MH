
////////////////////////////////
/// usage : 1.	switches for debug log.
/// 
/// note  : 1.	
////////////////////////////////

#ifndef SMART_SZX_INVENTORY_ROUTING_LOG_SWITCH_H
#define SMART_SZX_INVENTORY_ROUTING_LOG_SWITCH_H


#include "Utility.h"


namespace szx {

	struct LogSwitch {
		// TODO[szx][0]: turn off all logs before the release.

	  //  enum Szx {
	  //      Main = Log::Level::Info,
	  //      Cli = Log::Level::Off,
	  //      Framework = Log::Level::Off,
	  //      Input = Log::Level::On,
	  //      Output = Log::Level::Off,
	  //      Preprocess = Log::Level::Off,
	  //      Postprocess = Log::Level::Off,
	  //      Config = Log::Level::Off,
	  //      Model = Log::Level::On,
	  //      MpSolver = Log::Level::Off,
	  //      Checker = Log::Level::On,
			//Search=Log::Level::On,
			//Opt=Log::Level::On,
	  //  };

		enum Szx {
			Main = Log::Level::Info,
			Cli = Log::Level::Off,
			Framework = Log::Level::Off,
			Input = Log::Level::On,
			Output = Log::Level::Off,
			Preprocess = Log::Level::Off,
			Postprocess = Log::Level::Off,
			Config = Log::Level::Off,
			Model = Log::Level::Off,
			MpSolver = Log::Level::Off,
			Checker = Log::Level::On,
			Search = Log::Level::Off,
			Opt = Log::Level::Off,
		};
	};

}


#endif // SMART_SZX_INVENTORY_ROUTING_LOG_SWITCH_H