#pragma once

#include <string>
#include <string_view>

#include "er/result.hpp"
#include "er/Flags4096.hpp"

namespace er {

class Element {
public:
    static Result<Element> create(std::string name);
    Element() = default;

    [[nodiscard]] Result<Unit> set_name(std::string name);
    std::string_view name() const noexcept;

    Flags4096& flags();
    const Flags4096& flags() const;

private:
    explicit Element(std::string name);
    std::string name_;   // max 100 (enforced)
    Flags4096 flags_;
};

} // namespace er
