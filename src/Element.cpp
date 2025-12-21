#include "er/Element.hpp"
#include <stdexcept>

namespace er {

static void check_name(const std::string& n) {
    if (n.size() > 100) {
        throw std::length_error("Element name exceeds 100 characters");
    }
}

Element::Element(std::string name) : name_(std::move(name)) {
    check_name(name_);
}

void Element::set_name(const std::string& name) {
    check_name(name);
    name_ = name;
}

const std::string& Element::name() const {
    return name_;
}

Flags4096& Element::flags() {
    return flags_;
}

const Flags4096& Element::flags() const {
    return flags_;
}

} // namespace er
