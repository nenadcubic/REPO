#pragma once

#include <string>
#include "er/Flags4096.hpp"

namespace er {

class Element {
public:
    explicit Element(std::string name = "");

    void set_name(const std::string& name);
    const std::string& name() const;

    Flags4096& flags();
    const Flags4096& flags() const;

private:
    std::string name_;   // max 100 (enforced)
    Flags4096 flags_;
};

} // namespace er
