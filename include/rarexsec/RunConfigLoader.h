#ifndef RUN_CONFIG_LOADER_H
#define RUN_CONFIG_LOADER_H

#include <string>

#include <rarexsec/RunConfigRegistry.h>

namespace proc {

class RunConfigLoader {
  public:
    static void loadFromFile(const std::string &config_path, RunConfigRegistry &registry);
};

}

#endif // RUN_CONFIG_LOADER_H
