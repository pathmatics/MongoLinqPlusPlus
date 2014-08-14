using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UnitTests
{
    /// <summary>Dummy class for serialization tests</summary>
    public class DummyClass
    {
        // ReSharper disable once NotAccessedField.Local
        public int Id { get; set; }

        // ReSharper disable once NotAccessedField.Local
        public string MyName;

        // ReSharper disable once UnusedAutoPropertyAccessor.Local
        public int MyNumPets { get; set; }
    }
}
