
#ifndef _SYSCALL_CHECKER_
#define _SYSCALL_CHECKER_

//----------------------------------------------------------------------------------------------------------------------

#include <system_error>
#include <sstream>

//----------------------------------------------------------------------------------------------------------------------

namespace errors
{
    class syscall_result_failed : public std::exception
    {
    private:
        std::string  _condition;
        int          _line;
        std::string  _msg;

    public:
        inline syscall_result_failed(std::string condition, int line) : _condition(condition), _line(line)
        {
            std::ostringstream msg;
            msg <<  "check_error failed on line " << line << ": " << condition << "\t [ errno = " << errno << ": ";
            msg << std::system_error(errno, std::system_category()).what() << " ]";

            _msg = msg.str();
        }

        inline virtual const char* what() const throw () {
            return _msg.c_str();
        }
    };

#define syscall_check(result)  if ((result) < 0)  throw errors::syscall_result_failed(#result, __LINE__)

}

//----------------------------------------------------------------------------------------------------------------------

#endif
