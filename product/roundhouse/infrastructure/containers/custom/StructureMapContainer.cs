using System;

namespace roundhouse.infrastructure.containers.custom
{
    using StructureMap;

    public sealed class StructureMapContainer : IInversionContainer
    {
        private readonly IContainer _theContainer;

        public StructureMapContainer(IContainer theContainer)
        {
            if (theContainer == null) throw new ArgumentNullException(nameof(theContainer));
            _theContainer = theContainer;
        }

        public TypeToReturn Resolve<TypeToReturn>()
        {
            return _theContainer.GetInstance<TypeToReturn>();
        }
    }
}